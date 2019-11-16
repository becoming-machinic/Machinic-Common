/*
 * Copyright (C) 2019 Becoming Machinic Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.becomingmachinic.kafka.collections;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

/**
 * A CollectionSerde provides the ability to convert the collection object type to a primitive Kafka type.
 * 
 * @param <S>
 *          Raw Kafka type
 * @param <T>
 *          Collection object type
 */
public interface CollectionSerde<S,T> {

    /**
     * Convert an instance of the collection type to the raw kafka type
     * @param value Collection value
     * @return Raw Kafka value
     */
    public S serialize(T value) throws SerializationException;

    /**
     * Convert a raw kafka value into the collection type
     * @param raw
     * @return
     */
    public T deserialize(S raw) throws SerializationException;

    /**
     * get an instance of the correct Kafka serializer
     * @return
     */
    public Serializer<S> getRawSerializer();

    /**
     * Get an instance of the correct Kafka deserializer
     * @return
     */
    public Deserializer<S> getRawDeserializer();

    public static CollectionStringSerde<String> stringToString(){
        return new StringToStringCollectionSarde();
    }
    public static CollectionByteArraySerde<byte[]> byteArrayToByteArray(){
        return new ByteArrayToByteArrayCollectionSerde();
    }
    public static CollectionByteArraySerde<Hash> byteArrayToHash(){
        return new ByteArrayToHashCollectionSerde();
    }
}
