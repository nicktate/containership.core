'use strict';

const crypto = require('./crypto');

const _ = require('lodash');
const async = require('async');
const constants = require('containership.core.constants');
const flatten = require('flat');
const Myriad = require('myriad-kv');
const CodexD = require('codexd');
const Ohai = require('ohai-data');

const OHAI_ATTRIBUTES = [
    'os',
    'os_version',
    'platform',
    'platform_version',
    'platform_build',
    'platform_family',
    'virtualization.system',
    'virtualization.role'
];

class Cluster {
    constructor(core) {
        this.core = core;

        // register low-level loggers
        this.core.logger.register('myriad-kv');
        this.core.logger.register('praetor');
        this.core.logger.register('legiond');

        // ensure connect token validity if enabled
        if(core.options['connect-token']) {
            core.options.legiond.gatekeeper = function(data, callback) {
                if(data.metadata && data.metadata.containership && data.metadata.containership.connect_token) {
                    const decrypted_random_token = crypto.decrypt(core.options['connect-token'], data.metadata.containership.connect_token.encrypted);
                    const valid_connect_token = data.metadata.containership.connect_token.decrypted === decrypted_random_token;

                    if(valid_connect_token) {
                        return callback();
                    } else {
                        return callback(new Error(`Rejected connection attempt! ${data.id} has invalid connect token`));
                    }
                } else {
                    return callback(new Error(`Rejected connection attempt! ${data.id} has missing containership connect token metadata`));
                }
            };
        }

        const options = {
            leader_eligible: core.options.praetor.leader_eligible,
            legiond: core.options.legiond,
            persistence: core.options.persistence,
            logger: this.core.loggers['myriad-kv'],
            praetor: {
                logger: this.core.loggers.praetor
            }
        };

        options.legiond.attributes.start_time = new Date().valueOf();

        this.myriad = new Myriad(options);
    }

    initialize(initialize_callback) {
        const self = this;

        // wait until legiond is listening
        this.myriad.listen(() => {
            // makes legiond and praetor more easily accessible
            self.legiond = self.myriad.cluster.legiond;
            self.praetor = self.myriad.cluster.praetor;

            // creates an instance of CodexD
            self.codexd = new CodexD({
                legiond: self.legiond
            });

            // get existing attributes
            const attributes = self.legiond.get_attributes();

            // attempt to set Ohai tags
            Ohai.detect((err, data) => {
                const system_attributes = {};

                if(!err) {
                    const flat_data = flatten(data);
                    _.forEach(OHAI_ATTRIBUTES, (key) => {
                        if (_.has(flat_data, key)) {
                            system_attributes[key] = flat_data[key];
                        }
                    });
                }

                // merge host attributes
                const tags = _.defaults({
                    host: attributes.id,
                    host_name: attributes.host_name
                }, attributes.tags);

                // add ohai attributes if present
                if(!_.isEmpty(system_attributes)) {
                    tags.system = system_attributes;
                }

                // save updated node attributes
                self.legiond.set_attributes({
                    tags: tags
                });

                return initialize_callback();
            });

            // join channels
            _.forEach(self.core.options.channels, (channel) => {
                self.legiond.join(channel);
            });

            // drop praetor errors
            self.praetor.on('error', (/*err*/) => {});

            self.legiond.on('error', (err) => {
                let log_level = 'error';

                if(err.code === 'ECONNREFUSED') {
                    log_level = 'silly';
                }

                self.core.loggers.legiond.log(log_level, err.message);
            });

            // handle promotion
            self.legiond.on('promoted', (/*data*/) => {
                self.core.loggers['containership.core'].log('info', 'Promoted to controlling leader!');

                const peers = self.legiond.get_peers();

                self.core.loggers['containership.core'].log('verbose', `Controlling cluster of ${_.values(peers).length} nodes`);
                _.forEach(peers, (host/*, id*/) => {
                    self.core.loggers['containership.core'].log('debug', `Found ${host.mode} node: ${host.id}`);
                });

                setTimeout(() => {
                    if(self.praetor.is_controlling_leader()) {
                        self.core.scheduler.harmonize();
                    }
                }, 15000);
            });

            // handle myriad bootstrapping from snapshot
            self.legiond.on('myriad.bootstrapped', () => {
                self.calculate_cluster_id((err, cluster_id) => {
                    if(err) {
                        self.core.loggers['containership.core'].log('error', `Error calculating cluster id: ${err}`);
                    }

                    self.sync_cluster_id(cluster_id, (err) => {
                        if(err) {
                            self.core.loggers['containership.core'].log('error', `Error syncing cluster id: ${err}`);
                        }

                        setTimeout(() => {
                            if(self.praetor.is_controlling_leader()) {
                                self.core.scheduler.leader.application.start_all_health_checks((err) => {
                                    if(err) {
                                        self.core.loggers['containership.core'].log('error', `Error starting health checks: ${err}`);
                                    }
                                });
                                self.core.applications.bootstrap();
                            }
                        }, 2000);
                    });
                });
            });

            // handle demotion
            self.legiond.on('demoted', () => {
                self.core.loggers['containership.core'].log('info', 'Demoted from controlling leader!');
                self.core.scheduler.deharmonize();
                // delete health check processes
                self.core.scheduler.leader.application.stop_all_health_checks();
            });

            // handle new nodes
            self.legiond.on('node_added', (node) => {
                if(self.praetor.is_controlling_leader()) {
                    self.core.loggers['containership.core'].log('info', `Added node ${node.host_name}`);

                    // explicitly send cluster_id to new node to set in core
                    self.legiond.send({
                        event: constants.events.CLUSTER_ID,
                        data: {
                            cluster_id: self.core.cluster_id
                        }
                    }, [node]);

                    // send reconcile event to new node
                    self.legiond.send({
                        event: constants.events.RECONCILE,
                        data: {
                            cluster_id: self.core.cluster_id
                        }
                    }, [node]);
                }
            });

            // handle removed nodes
            self.legiond.on('node_removed', (node) => {
                if(self.praetor.is_controlling_leader()) {
                    self.core.loggers['containership.core'].log('info', `Removed node ${node.host_name}`);

                    self.myriad.persistence.keys([constants.myriad.CONTAINERS_PREFIX, '*', '*'].join(constants.myriad.DELIMITER), (err, container_names) => {
                        let containers = [];

                        async.each(container_names, (container_name, callback) => {
                            self.myriad.persistence.get(container_name, (err, container) => {
                                if(_.isNull(err)) {
                                    try {
                                        container = JSON.parse(container);
                                        container.application = container_name.split(constants.myriad.DELIMITER)[2];
                                        if(container.host === node.id) {
                                            containers.push(container);
                                        }

                                        return callback();
                                    } catch(err) {
                                        return callback();
                                    }
                                } else {
                                    return callback();
                                }
                            });
                        }, () => {
                            async.each(containers, (container, callback) => {
                                self.core.applications.unload_containers(container.application, container.id, callback);
                            }, () => {});
                        });
                    });
                }
            });

            // handle loading containers
            self.legiond.on(constants.events.LOAD_CONTAINER, (message) => {
                self.core.scheduler.follower.container.start(message.data);
            });

            // handle unloading containers
            self.legiond.on(constants.events.UNLOAD_CONTAINER, (message) => {
                self.core.scheduler.follower.container.stop(message.data);
            });

            // handle reconciling running application containers
            self.legiond.on(constants.events.RECONCILE, (message) => {
                self.core.scheduler.follower.container.reconcile(message.author);
            });

            // set the cluster id
            self.legiond.on(constants.events.CLUSTER_ID, (message) => {
                self.core.cluster_id = message.data.cluster_id;
            });

            // update host
            self.legiond.on(constants.events.UPDATE_HOST, (message) => {
                self.core.loggers['containership.core'].log('info', 'Updating host with new tags');
                self.legiond.set_attributes({
                    tags: message.data
                });
            });

            // delete host
            self.legiond.on(constants.events.DELETE_HOST, () => {
                self.core.loggers['containership.core'].log('info', 'Host shutdown requested. Shutting down ...');
                self.legiond.exit(() => {
                    process.exit(0);
                });
            });
        });
    }

    sync_cluster_id(cluster_id, callback) {
        this.myriad.persistence.set(constants.myriad.CLUSTER_ID, cluster_id, (err) => {
            if(err) {
                return callback(err);
            }

            this.core.cluster.legiond.send({
                event: constants.events.CLUSTER_ID,
                data: {
                    cluster_id: cluster_id
                }
            });

            return callback();
        });
    }

    calculate_cluster_id(callback) {
        this.myriad.persistence.get(constants.myriad.CLUSTER_ID, (err, cluster_id) => {
            if(err && err.name !== 'ENOKEY') {
                process.stderr.write(err.message);
                process.exit(1);
            } else if(err && err.name === 'ENOKEY') {
                this.core.cluster_id = this.core.options.cluster_id || crypto.generate_uuid();
                return callback(null, this.core.cluster_id);
            } else {
                this.core.cluster_id = cluster_id;
                return callback(null, this.core.cluster_id);
            }
        });
    }

}

module.exports = Cluster;
