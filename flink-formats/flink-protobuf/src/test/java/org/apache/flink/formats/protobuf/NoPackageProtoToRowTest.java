/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.protobuf;

import org.apache.flink.formats.protobuf.util.PbFormatUtils;
import org.apache.flink.table.data.RowData;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test class for below case
 *
 * <PRE>
 * syntax = "proto3";
 * option java_outer_classname = "UserProtoBuf";
 * message User {
 * </PRE>
 *
 * <p>The .proto file declares neither {@code package} nor {@code option java_package}, so the
 * generated {@code UserProtoBuf} class lives in Java's unnamed package, exactly like the reporter's
 * case in FLINK-35034. {@code UserProtoBuf$User} is therefore referenced here only by name (via
 * reflection), the same way {@link PbFormatUtils#getDescriptor} and the runtime {@code
 * PbFormatConfig#getMessageClassName()} path resolve it; a class in a named package cannot {@code
 * import} a class from the unnamed package.
 *
 * <p>Before the fix, building the deserialization schema for this descriptor failed with {@code
 * org.codehaus.janino.Java.CompileException: IDENTIFIER expected instead of '.'} because {@link
 * PbFormatUtils#getOuterProtoPrefix} generated the leading-dot type reference {@code
 * .UserProtoBuf.User}.
 */
class NoPackageProtoToRowTest {
    @Test
    void testSimple() throws Exception {
        Class<?> messageClass = Class.forName("UserProtoBuf$User");
        Descriptors.Descriptor descriptor = PbFormatUtils.getDescriptor(messageClass.getName());
        DynamicMessage message =
                DynamicMessage.newBuilder(descriptor)
                        .setField(descriptor.findFieldByName("id"), 42)
                        .setField(descriptor.findFieldByName("name"), "flink")
                        .build();

        RowData row = ProtobufTestHelper.pbBytesToRow(messageClass, message.toByteArray());

        assertThat(row.getInt(0)).isEqualTo(42);
        assertThat(row.getString(1).toString()).isEqualTo("flink");
    }
}
