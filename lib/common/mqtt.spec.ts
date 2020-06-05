/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

import { v4 as uuid } from 'uuid';

import { ClientBootstrap } from '@awscrt/io';
import { MqttClient, QoS, MqttWill } from '@awscrt/mqtt';
import { AwsIotMqttConnectionConfigBuilder } from '@awscrt/aws_iot';
import { TextDecoder } from '@awscrt/polyfills';
import { Config, fetch_credentials } from '@test/credentials';

jest.setTimeout(10000);

test('MQTT Connect/Disconnect', async () => {
    let aws_opts: Config;
    try {
        aws_opts = await fetch_credentials();
    } catch (err) {
        return;
    }

    const config = AwsIotMqttConnectionConfigBuilder.new_mtls_builder(aws_opts.certificate, aws_opts.private_key)
        .with_clean_session(true)
        .with_client_id(`node-mqtt-unit-test-${uuid()}`)
        .with_endpoint(aws_opts.endpoint)
        .with_credentials(Config.region, aws_opts.access_key, aws_opts.secret_key, aws_opts.session_token)
        .build()
    const client = new MqttClient(new ClientBootstrap());
    const connection = client.new_connection(config);
    const promise = new Promise((resolve, reject) => {
        connection.on('connect', (session_present) => {
            connection.disconnect();

            if (session_present) {
                reject("Session present");
            }

        });
        connection.on('error', (error) => {
            reject(error);

        })
        connection.on('disconnect', () => {
            resolve(true);
        })
        connection.connect();
    });
    await expect(promise).resolves.toBeTruthy();
});

test('MQTT Pub/Sub', async () => {

    let aws_opts: Config;
    try {
        aws_opts = await fetch_credentials();
    } catch (err) {
        return;
    }

    const decoder = new TextDecoder('utf8');
    const config = AwsIotMqttConnectionConfigBuilder.new_mtls_builder(aws_opts.certificate, aws_opts.private_key)
        .with_clean_session(true)
        .with_client_id(`node-mqtt-unit-test-${uuid()}`)
        .with_endpoint(aws_opts.endpoint)
        .with_credentials(Config.region, aws_opts.access_key, aws_opts.secret_key, aws_opts.session_token)
        .with_timeout_ms(5000)
        .build()
    const client = new MqttClient(new ClientBootstrap());
    const connection = client.new_connection(config);
    const promise = new Promise((resolve, reject) => {
        connection.on('connect', async (session_present) => {
            expect(session_present).toBeFalsy();
            const test_topic = '/test/me/senpai';
            const test_payload = 'NOTICE ME';
            connection.subscribe(test_topic, QoS.AtLeastOnce, (topic, payload) => {
                connection.disconnect();

                if (topic != test_topic) {
                    reject("Topic does not match");
                }
                if (payload === undefined) {
                    reject("Undefined payload");
                }
                const payload_str = decoder.decode(payload);
                if (payload_str !== test_payload) {
                    reject("Payloads do not match");
                }
                resolve(true);
            });
            connection.publish(test_topic, test_payload, QoS.AtLeastOnce);
        });
        connection.on('error', (error) => {
            reject(error);
        })
        connection.connect();
    });
    await expect(promise).resolves.toBeTruthy();
});

test('MQTT Will', async () => {
    let aws_opts: Config;
    try {
        aws_opts = await fetch_credentials();
    } catch (err) {
        return;
    }

    const config = AwsIotMqttConnectionConfigBuilder.new_mtls_builder(aws_opts.certificate, aws_opts.private_key)
        .with_clean_session(true)
        .with_client_id(`node-mqtt-unit-test-${uuid()}`)
        .with_endpoint(aws_opts.endpoint)
        .with_credentials(Config.region, aws_opts.access_key, aws_opts.secret_key, aws_opts.session_token)
        .with_will(new MqttWill(
            '/last/will/and/testament',
            QoS.AtLeastOnce,
            'AVENGE ME'
        ))
        .build()
    const client = new MqttClient(new ClientBootstrap());
    const connection = client.new_connection(config);
    const promise = new Promise((resolve, reject) => {
        connection.on('connect', (session_present) => {
            connection.disconnect();

            if (session_present) {
                reject("Session present");
            }
        });
        connection.on('error', (error) => {
            reject(error)
        })
        connection.on('disconnect', () => {
            resolve(true);
        })
        connection.connect();
    });
    await expect(promise).resolves.toBeTruthy();
});

test('MQTT On Any Publish', async () => {
    let aws_opts: Config;
    try {
        aws_opts = await fetch_credentials();
    } catch (err) {
        return;
    }

    const decoder = new TextDecoder('utf8');
    const config = AwsIotMqttConnectionConfigBuilder.new_mtls_builder(aws_opts.certificate, aws_opts.private_key)
        .with_clean_session(true)
        .with_client_id(`node-mqtt-unit-test-${uuid()}`)
        .with_endpoint(aws_opts.endpoint)
        .with_credentials(Config.region, aws_opts.access_key, aws_opts.secret_key, aws_opts.session_token)
        .with_timeout_ms(5000)
        .build()
    const client = new MqttClient(new ClientBootstrap());
    const connection = client.new_connection(config);
    const promise = new Promise((resolve, reject) => {
        const test_topic = '/test/me/senpai';
        const test_payload = 'NOTICE ME';
        // have to subscribe or else the broker won't send us the message
        connection.subscribe(test_topic, QoS.AtLeastOnce);
        connection.on('message', (topic, payload) => {
            connection.disconnect();
            if (topic != test_topic) {
                reject("Topic does not match");
            }
            if (payload === undefined) {
                reject("Undefined payload");
            }
            const payload_str = decoder.decode(payload);
            if (payload_str !== test_payload) {
                reject("Payloads do not match");
            }

            resolve(true);
        });
        connection.on('connect', async (session_present) => {
            expect(session_present).toBeFalsy();
            connection.publish(test_topic, test_payload, QoS.AtLeastOnce);
        });
        connection.on('error', (error) => {
            reject(error);
        })
        connection.connect();
    });
    await expect(promise).resolves.toBeTruthy();
});
