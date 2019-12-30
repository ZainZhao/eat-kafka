/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients;

import org.apache.kafka.common.config.ConfigException;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;

public class ClientUtilsTest {

    @Test
    public void testParseAndValidateAddresses() {
        check("127.0.0.1:8000");
        check("mydomain.com:8080");
        check("[::1]:8000");
        check("[2001:db8:85a3:8d3:1319:8a2e:370:7348]:1234", "mydomain.com:10000");
        List<InetSocketAddress> validatedAddresses = check("some.invalid.hostname.foo.bar:9999", "mydomain.com:10000");
        assertEquals(1, validatedAddresses.size());
        InetSocketAddress onlyAddress = validatedAddresses.get(0);
        assertEquals("mydomain.com", onlyAddress.getHostName());
        assertEquals(10000, onlyAddress.getPort());
    }

    @Test(expected = ConfigException.class)
    public void testNoPort() {
        check("127.0.0.1");
    }
    
    @Test(expected = ConfigException.class)
    public void testOnlyBadHostname() {
        check("some.invalid.hostname.foo.bar:9999");
    }

    private List<InetSocketAddress> check(String... url) {
        return ClientUtils.parseAndValidateAddresses(Arrays.asList(url));
    }
}
