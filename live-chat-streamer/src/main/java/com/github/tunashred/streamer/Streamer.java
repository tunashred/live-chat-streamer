package com.github.tunashred.streamer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.tunashred.streamer.util.Util;
import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import lombok.experimental.FieldDefaults;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@Data
@Log4j2
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class Streamer {
    static String PREFERENCES_TOPIC = "streamer-preferences";
    @Getter
    static Map<String, List<String>> preferencesMap = new HashMap<>();
    static KafkaProducer<String, String> producer = null;
    static KafkaStreams streams = null;

    public Streamer(String inputTopic, String producerPropertiesPath, String streamsPropertiesPath) {
        log.info("Loading producer properties");
        Properties producerProps = new Properties();
        try (InputStream propsFile = new FileInputStream(producerPropertiesPath)) {
            producerProps.load(propsFile);
            producer = new KafkaProducer<>(producerProps);
        } catch (IOException e) {
            log.error("Failed to load producer properties file: ", e);
            return;
        }

        log.info("Loading streams properties");
        Properties streamsProps = new Properties();
        try (InputStream propsFile = new FileInputStream(streamsPropertiesPath)) {
            streamsProps.load(propsFile);
        } catch (IOException e) {
            log.error("Unable to load streams properties", e);
            return;
        }
        log.info("Initializing streamer KafkaStreams");
        streams = new KafkaStreams(createTopology(inputTopic, preferencesMap), streamsProps);
    }

    public static Topology createTopology(String inputTopic, Map<String, List<String>> preferencesMap) {
        log.info("Creating topology");
        StreamsBuilder builder = new StreamsBuilder();

        KTable<String, String> userTable = builder.table(
                inputTopic,
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as(inputTopic + "-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.String())
        );

        userTable.toStream().foreach((key, value) -> {
            if (value != null) {
                try {
                    log.trace("Receiving preferences for streamer + '{}'", key);
                    List<String> preferences = Util.deserializeList(value);
                    preferencesMap.put(key, preferences);
                    log.trace("Preferences list added: {}", preferences);
                } catch (JsonProcessingException e) {
                    log.error("Error ocurred while trying to deserialize streamer '{}' preferences list", key);
                }
            }
        });

        return builder.build();
    }

    public static boolean addPreference(String channel, String pack) throws JsonProcessingException {
        if (!preferencesMap.containsKey(channel)) {
            log.error("Unknown streamer with name '{}'", channel);
            return false;
        }
        List<String> preferences = preferencesMap.get(channel);
        if (preferences.isEmpty()) {
            log.error("Streamer has no pack preferences");
            return false;
        }
        preferences.add(pack);

        producer.send(new ProducerRecord<>(PREFERENCES_TOPIC, channel, Util.serializeList(preferences)));
        log.info("Pack '{}' added to streamer '{}' preferences", pack, channel);
        return true;
    }

    public static boolean removePreference(String channel, String pack) throws JsonProcessingException {
        if (!preferencesMap.containsKey(channel)) {
            log.error("Unknown streamer with name '{}'", channel);
            return false;
        }
        List<String> preferences = preferencesMap.get(channel);
        if (preferences.isEmpty()) {
            log.error("Streamer has no pack preferences");
            return false;
        }
        if (!preferences.remove(pack)) {
            log.error("Streamer '{}' does not have the pack '{}' in their preferences", channel, pack);
            return false;
        }
        producer.send(new ProducerRecord<>(PREFERENCES_TOPIC, channel, Util.serializeList(preferences)));
        log.info("Pack '{}' removed from streamer '{}' preferences", pack, channel);
        return true;
    }

    public void start() {
        log.info("Starting streamer");
        streams.setStateListener(((newState, oldState) -> {
            if (newState == KafkaStreams.State.RUNNING && oldState != KafkaStreams.State.RUNNING) {
                log.info("Streamer ready");
            }
        }));
        streams.start();
    }

    public void close() {
        streams.close();
    }
}
