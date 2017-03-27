'use strict';

const Applications = require('./lib/applications');
const Cluster = require('./lib/cluster');
const crypto = require('./lib/crypto');
const Logger = require('./lib/logger');
const plugins = require('./lib/plugins');

const constants = require('containership.core.constants');
const os = require('os');

const _ = require('lodash');

class ContainerShipCore {
    constructor(options) {
        this.load_options(options || {});
    }

    // initializes the core
    initialize() {
        const self = this;

        this.constants = constants;

        // initializes logger
        this.loggers = {};
        this.logger = new Logger(this);
        this.logger.register('containership.core');

        // initializes applications on leader nodes
        if(this.options.mode == 'leader') {
            this.applications = new Applications(this);
        }

        this.loggers['containership.core'].log('info', `Containership version ${this.options.version} started in ${this.options.mode} mode!`);

        // initialize cluster
        this.cluster = new Cluster(this);
        this.cluster.initialize(() => {
            self.scheduler.load_core(self);

            if(self.options.mode == 'leader') {
                self.api.load_core(self);
                self.api.server.start(self.options);
            }

            plugins.initialize(self);
            plugins.load();

            // handle process reload
            process.on('SIGHUP', plugins.reload);

            const handle_process_exit = (/* code */) => {
                plugins.stop();
                this.cluster.legiond.exit(() => {
                    process.exit(0);
                });
            };

            // handle process exit
            process.on('SIGTERM', handle_process_exit);
            process.on('SIGINT', handle_process_exit);
        });
    }

    // loads and sets options
    load_options(options) {
        options = _.merge(options, {
            praetor: {
                leader_eligible: false
            },
            legiond: {
                attributes: {
                    mode: 'follower'
                },
                gatekeepers: [],
                network: {}
            },
            channels: []
        });

        options.legiond.network.cidr = options.cidr;
        options.legiond.network.public = options['legiond-scope'] == 'public';

        if(_.has(options, 'legiond-interface')) {
            options.legiond.network.interface = options['legiond-interface'];
        }

        if(_.has(options, 'cluster-id')) {
            options.cluster_id = options['cluster-id'];
        }

        if(_.has(options, 'node-id')) {
            options.legiond.network.id = options['node-id'];
        }

        if(options.mode == 'leader') {
            options.legiond.attributes.mode = 'leader';
            options.praetor.leader_eligible = true;
            options.legiond.attributes.tags = {};
            _.forEach(options.tag, (tag) => {
                options.legiond.attributes.tags[tag.tag] = tag.value;
            });
            options.channels = [
                constants.events.CLUSTER_ID
            ];
        } else {
            options.legiond.attributes.engines = {};
            options.legiond.attributes.tags = {};
            _.forEach(options.tag, (tag) => {
                options.legiond.attributes.tags[tag.tag] = tag.value;
            });
            options.channels = [
                constants.events.CLUSTER_ID,
                constants.events.RECONCILE,
                constants.events.LOAD_CONTAINER, constants.events.UNLOAD_CONTAINER,
                constants.events.UPDATE_HOST,
                constants.events.DELETE_HOST
            ];

            const host_memory = Math.floor(os.totalmem());
            const host_cpus = os.cpus().length;
            options.legiond.attributes.memory = options['scheduler-available-memory'] &&  options['scheduler-available-memory'] <= host_memory ? options['scheduler-available-memory'] : host_memory;
            options.legiond.attributes.cpus = options['scheduler-available-cpus'] && options['scheduler-available-cpus'] <= host_cpus ? options['scheduler-available-cpus'] : host_cpus;
        }

        options.legiond.attributes.metadata = {
            containership: {
                version: options.version
            }
        };

        if(options['connect-token']) {
            const random_token = crypto.generate_uuid();

            options.legiond.attributes.metadata.containership.connect_token = {
                encrypted: crypto.encrypt(options['connect-token'], random_token),
                decrypted: random_token
            };
        }

        options.persistence = {
            max_coalescing_duration: 1024,
            data_directory: '/tmp',
            snapshot_name: 'containership.snapshot'
        };

        if(_.has(options, 'snapshot-location')) {
            options.persistence.data_directory = options['snapshot-location'].substring(0, options['snapshot-location'].lastIndexOf('/'));
            options.persistence.snapshot_name = options['snapshot-location'].substring(options['snapshot-location'].lastIndexOf('/') + 1);
        }

        this.options = options;
    }

}

module.exports = ContainerShipCore;
