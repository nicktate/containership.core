'use strict';

const constants = require('./constants');
const crypto = require('./crypto');

const _ = require('lodash');
const async = require('async');
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
        this.core.logger.register('myriad-kv');
        this.core.logger.register('praetor');

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
                    host_name: attributes.host_name,
                }, attributes.tags);

                // add ohai attributes if present
                if(!_.isEmpty(system_attributes)) {
                    tags.system = system_attributes;
                }

                // save updated node attributes
                self.legiond.set_attributes({
                    tags: tags
                });

                self.set_id(initialize_callback);
            });

            // join channels
            _.forEach(self.core.options.channels, (channel) => {
                self.legiond.join(channel);
            });

            self.praetor.on('error', (/*err*/) => {});

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
                setTimeout(() => {
                    self.core.applications.bootstrap();
                }, 2000);
            });

            // handle demotion
            self.legiond.on('demoted', () => {
                self.core.loggers['containership.core'].log('info', 'Demoted from controlling leader!');
                self.core.scheduler.deharmonize();
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
                // if cluster_id explicitly passed, just set from message
                if (message && message.data && message.data.cluster_id) {
                    self.core.cluster_id = message.data.cluster_id;
                } else {
                    // set from myriad
                    self.set_id();
                }
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

    generate_id(callback) {
        this.myriad.persistence.set(constants.myriad.CLUSTER_ID, crypto.generate_uuid(), callback);
    }

    set_id(callback) {
        const self = this;

        this.myriad.persistence.get(constants.myriad.CLUSTER_ID, (err, cluster_id) => {
            if(_.isNull(err)) {
                self.core.cluster_id = cluster_id;
            }

            return callback(err);
        });
    }

}


module.exports = Cluster;
