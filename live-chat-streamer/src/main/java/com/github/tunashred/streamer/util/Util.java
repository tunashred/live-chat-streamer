package com.github.tunashred.streamer.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.List;

public class Util {
    public static String serializeList(List<String> list) throws JsonProcessingException {
        ObjectMapper writer = new ObjectMapper();
        return writer.writeValueAsString(list);
    }

    public static List<String> deserializeList(String preferences) throws JsonProcessingException {
        ObjectMapper reader = new ObjectMapper();
        return reader.readValue(preferences, new TypeReference<ArrayList<String>>() {
        });
    }
}
