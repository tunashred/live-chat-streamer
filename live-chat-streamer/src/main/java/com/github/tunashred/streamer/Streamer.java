package com.github.tunashred.streamer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.tunashred.streamer.util.Util;
import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import lombok.experimental.FieldDefaults;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.regex.Pattern;

@Data
@Log4j2
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
// TODO: will I benefit from making the consumer at-most-once?
public class Streamer {
    static String PREFERENCES_TOPIC = "streamer-preferences";
    @Getter
    static Map<String, List<String>> preferencesMap = new HashMap<>();
    static KafkaProducer<String, String> producer = null;
    static KafkaConsumer<String, String> consumer = null;
    static KafkaStreams streams = null;
    String topic;

    public Streamer(String inputTopic, String consumerPropertiesPath, String producerPropertiesPath, String streamsPropertiesPath) {
        this(inputTopic, consumerPropertiesPath, producerPropertiesPath, streamsPropertiesPath, new Properties(), new Properties());
    }

    public Streamer(String inputTopic, String consumerPropertiesPath, String producerPropertiesPath,
                    String streamsPropertiesPath, Properties consumerProperties, Properties streamsProperties) {
        // TODO: not my proudest rodeo with properties
        this.topic = inputTopic;
        log.info("Loading consumer properties");
        Properties consumerProps = new Properties();
        try (InputStream propsFile = new FileInputStream(consumerPropertiesPath)) {
            consumerProps.load(propsFile);
            consumerProps.putAll(consumerProperties);
            consumer = new KafkaConsumer<>(consumerProps);
            Pattern pattern = Pattern.compile("^pack-.*");
            consumer.subscribe(pattern);
        } catch (IOException e) {
            log.error("Failed to load consumer properties file: ", e);
            return;
        }

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
            streamsProps.putAll(streamsProperties);
        } catch (IOException e) {
            log.error("Unable to load streams properties", e);
            return;
        }
        log.info("Initializing streamer KafkaStreams");
        streams = new KafkaStreams(createTopology(this.topic, preferencesMap), streamsProps);
    }

    public static Topology createTopology(String inputTopic, Map<String, List<String>> preferencesMap) {
        log.info("Creating topology");
        StreamsBuilder builder = new StreamsBuilder();

        KTable<String, String> userTable = builder.table(
                inputTopic,
                Consumed.with(Serdes.String(), Serdes.String())
                        .withOffsetResetPolicy(Topology.AutoOffsetReset.EARLIEST),
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as(inputTopic + "-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.String())
                        .withCachingDisabled()
        );

        userTable.toStream().foreach((key, value) -> {
            log.trace("Receiving preferences for streamer + '{}'", key);
            if (value != null) {
                try {
                    List<String> preferences = Util.deserializeList(value);
                    preferencesMap.put(key, preferences);
                    log.trace("Preferences list added: {}", preferences);
                } catch (JsonProcessingException e) {
                    log.error("Error ocurred while trying to deserialize streamer '{}' preferences list", key, e);
                    log.error("Raw value: {}", value);
                }
            } else {
                log.trace("Preference list removed: {}", key);
            }
        });

        return builder.build();
    }

    public static List<String> listPacks() {
        List<String> packs = new ArrayList<>();
        Map<String, List<PartitionInfo>> topicMap = consumer.listTopics();
        for (String topic : topicMap.keySet()) {
            if (topic.startsWith("pack-")) {
                packs.add(topic);
            }
        }
        return packs;
    }

    public static boolean addPreference(String channel, String pack) throws JsonProcessingException {
        log.trace("Adding pack '{}' to channel '{}' preferences", pack, channel);
        String packName = packanizeChannelName(pack);
        List<String> availablePacks = listPacks();

        if (!availablePacks.contains(packName)) {
            log.error("Pack named '{}' does not exist", packName);
            return false;
        }
        // TODO: add check if topic exists
        if (!preferencesMap.containsKey(channel)) {
            log.warn("Unknown streamer with name '{}'", channel);
            preferencesMap.put(channel, new ArrayList<>());
        }
        List<String> preferences = preferencesMap.get(channel);
        if (preferences.isEmpty()) {
            log.warn("Streamer has no pack preferences");
        }

        if (preferences.contains(packName)) {
            log.warn("Streamer already has the pack '{}' inside their preferences", packName);
            return false;
        }
        preferences.add(packName);

        producer.send(new ProducerRecord<>(PREFERENCES_TOPIC, channel, Util.serializeList(preferences)));
        producer.flush();
        log.trace("Pack '{}' added to streamer '{}' preferences", pack, channel);
        return true;
    }

    public static boolean removePreference(String channel, String pack) throws JsonProcessingException {
        log.trace("Removing pack '{}' to channel '{}' preferences", pack, channel);
        String packName = packanizeChannelName(pack);
        if (!preferencesMap.containsKey(channel)) {
            log.error("Unknown streamer with name '{}'", channel);
            return false;
        }
        List<String> preferences = preferencesMap.get(channel);
        if (preferences.isEmpty()) {
            log.error("Streamer has no pack preferences");
            return false;
        }

        if (!preferences.remove(packName)) {
            log.error("Streamer '{}' does not have the pack '{}' in their preferences", channel, packName);
            return false;
        }
        producer.send(new ProducerRecord<>(PREFERENCES_TOPIC, channel, Util.serializeList(preferences)));
        log.info("Pack '{}' removed from streamer '{}' preferences", packName, channel);
        return true;
    }

    private static String packanizeChannelName(String topicName) {
        if (!topicName.startsWith("pack-")) {
            return "pack-" + topicName;
        }
        return topicName;
    }

    private void loadStoreManually(KafkaStreams streams) {
        streams.setStateListener(((newState, oldState) -> {
            if (newState == KafkaStreams.State.RUNNING && oldState != KafkaStreams.State.RUNNING) {
                log.info("Streamer is now running");
                loadStore(streams);
            }
        }));
    }

    private void loadStore(KafkaStreams streams) {
        final String storeName = topic + "-store";
        try {
            log.info("Trying to load manually the words into from store");
            ReadOnlyKeyValueStore<String, String> store =
                    streams.store(StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.keyValueStore()));

            int count = 0;
            KeyValue<String, String> entry = null;
            try (KeyValueIterator<String, String> iterator = store.all()) {
                while (iterator.hasNext()) {
                    entry = iterator.next();
                    String key = entry.key;
                    String value = entry.value;

                    log.trace("Manually added preference: {}", key);

                    if (value == null) {
                        preferencesMap.remove(key);
                    } else {
                        preferencesMap.put(key, Util.deserializeList(value));
                    }
                    count++;
                }
            } catch (JsonProcessingException e) {
                log.error("Error ocurred while trying to deserialize streamer '{}' preferences list", entry.key, e);
                log.error("Raw value: {}", entry.value);
            }

            if (preferencesMap == null || preferencesMap.isEmpty()) {
                log.warn("Streamer KTable store is empty");
                return;
            }
            log.trace("All preferences loaded successfully: {} preferences processed", count);
        } catch (InvalidStateStoreException e) {
            log.error("Failed to access store '{}': ", storeName, e);
        }
    }

    public void start() {
        log.info("Starting streamer");
        loadStoreManually(streams);
        streams.start();
    }

    public void close() {
        log.info("Closing streamer");
        streams.close();
    }
}
