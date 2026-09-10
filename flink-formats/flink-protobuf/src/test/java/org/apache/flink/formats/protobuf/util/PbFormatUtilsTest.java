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

package org.apache.flink.formats.protobuf.util;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link PbFormatUtils}. */
class PbFormatUtilsTest {

    /**
     * FLINK-35034: a .proto file with neither {@code package} nor {@code option java_package} must
     * not produce a leading-dot type reference.
     */
    @Test
    void testGetOuterProtoPrefixWithEmptyPackageAndOuterClass() throws Exception {
        Descriptors.FileDescriptor fileDescriptor = buildSyntheticFileDescriptor(false, "Outer");
        assertThat(PbFormatUtils.getOuterProtoPrefix(fileDescriptor)).isEqualTo("Outer.");
    }

    /** Same as above, but for the {@code java_multiple_files = true} branch. */
    @Test
    void testGetOuterProtoPrefixWithEmptyPackageAndMultipleFiles() throws Exception {
        Descriptors.FileDescriptor fileDescriptor = buildSyntheticFileDescriptor(true, null);
        assertThat(PbFormatUtils.getOuterProtoPrefix(fileDescriptor)).isEmpty();
    }

    /** Existing behaviour for a non-empty java package must be unchanged. */
    @Test
    void testGetOuterProtoPrefixWithNonEmptyPackageIsUnchanged() throws Exception {
        Descriptors.FileDescriptor fileDescriptor =
                (Descriptors.FileDescriptor)
                        Class.forName(
                                        "org.apache.flink.formats.protobuf.testproto.SimpleTestOuterNomultiProto")
                                .getMethod("getDescriptor")
                                .invoke(null);
        assertThat(PbFormatUtils.getOuterProtoPrefix(fileDescriptor))
                .isEqualTo(
                        "org.apache.flink.formats.protobuf.testproto.SimpleTestOuterNomultiProto.");
    }

    /**
     * Same assertion as {@link #testGetOuterProtoPrefixWithEmptyPackageAndOuterClass()} but against
     * the real generated descriptor for {@code test_no_package.proto}, which reproduces the
     * reporter's {@code UserProtoBuf.User} case exactly.
     */
    @Test
    void testGetOuterProtoPrefixMatchesRealNoPackageDescriptor() throws Exception {
        Descriptors.FileDescriptor fileDescriptor =
                (Descriptors.FileDescriptor)
                        Class.forName("UserProtoBuf").getMethod("getDescriptor").invoke(null);
        assertThat(PbFormatUtils.getOuterProtoPrefix(fileDescriptor)).isEqualTo("UserProtoBuf.");
    }

    private static Descriptors.FileDescriptor buildSyntheticFileDescriptor(
            boolean javaMultipleFiles, String javaOuterClassname) throws Exception {
        DescriptorProtos.FileOptions.Builder options =
                DescriptorProtos.FileOptions.newBuilder().setJavaMultipleFiles(javaMultipleFiles);
        if (javaOuterClassname != null) {
            options.setJavaOuterClassname(javaOuterClassname);
        }
        DescriptorProtos.FileDescriptorProto fileProto =
                DescriptorProtos.FileDescriptorProto.newBuilder()
                        .setName("synthetic_no_package.proto")
                        .setSyntax("proto3")
                        .setOptions(options.build())
                        .addMessageType(
                                DescriptorProtos.DescriptorProto.newBuilder()
                                        .setName("Foo")
                                        .build())
                        .build();
        return Descriptors.FileDescriptor.buildFrom(fileProto, new Descriptors.FileDescriptor[0]);
    }
}
