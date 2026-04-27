/*
 * ionet
 * Copyright (C) 2021 - present  渔民小镇 （262610965@qq.com、luoyizhu@gmail.com） . All Rights Reserved.
 * # iohao.com . 渔民小镇
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package com.iohao.net.server;

import com.iohao.net.framework.communication.*;
import com.iohao.net.framework.protocol.*;
import com.iohao.net.server.connection.*;
import io.aeron.*;
import io.aeron.logbuffer.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests net-server registration lifecycle behavior.
 *
 * @author 渔民小镇
 * @date 2026-04-26
 * @since 25.4
 */
class NetServerAgentTest {
    @Test
    void shouldRegisterFutureBeforePublishingServerToCenter() {
        var futureManager = new RecordingFutureManager();
        var connectionManager = new RecordingConnectionManager(futureManager);
        var setting = NetServerSetting.builder()
                .setNetId(1)
                .setConnectionManager(connectionManager)
                .setFutureManager(futureManager)
                .setListenerList(List.of())
                .setServerShutdownHookList(List.of())
                .build();

        var agent = new NetServerAgent(setting);
        var server = Server.recordBuilder()
                .setId(1001)
                .setName("logic")
                .setTag("logic")
                .setServerType(ServerTypeEnum.LOGIC)
                .setNetId(1)
                .setIp("127.0.0.1")
                .setPubName("1")
                .setCmdMerges(new int[0])
                .setPayloadMap(new HashMap<>())
                .build();

        agent.addServer(server);
        agent.onStart();

        assertTrue(connectionManager.published.get(), "server registration should be published");
    }

    private static final class RecordingFutureManager implements FutureManager {
        private final Map<Long, CompletableFuture<?>> futures = new ConcurrentHashMap<>();
        private final AtomicLong idGenerator = new AtomicLong(1);

        @Override
        public long nextFutureId() {
            return this.idGenerator.getAndIncrement();
        }

        @Override
        public <T> CompletableFuture<T> ofFuture(long futureId) {
            var future = new CompletableFuture<T>();
            this.futures.put(futureId, future);
            return future;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> CompletableFuture<T> remove(long futureId) {
            return (CompletableFuture<T>) this.futures.remove(futureId);
        }

        boolean contains(long futureId) {
            return this.futures.containsKey(futureId);
        }
    }

    private static final class RecordingConnectionManager implements ConnectionManager {
        private final RecordingFutureManager futureManager;
        private final AtomicBoolean published = new AtomicBoolean();

        private RecordingConnectionManager(RecordingFutureManager futureManager) {
            this.futureManager = futureManager;
        }

        @Override
        public void awaitConnect() {
        }

        @Override
        public void addConnection(ConnectionItem connection) {
        }

        @Override
        public void publishMessage(int serverId, Object message) {
        }

        @Override
        public void publishMessageByNetId(int netId, Object message) {
        }

        @Override
        public void publishMessageToCenter(Object message) {
            var serverMessage = assertInstanceOf(ServerRequestMessage.class, message);
            assertTrue(this.futureManager.contains(serverMessage.getFutureId()));
            this.published.set(true);
        }

        @Override
        public boolean containsNetId(int netId) {
            return false;
        }

        @Override
        public Publication getPublicationByNetId(int netId) {
            return null;
        }

        @Override
        public void publishMessage(String pubName, Object message) {
        }

        @Override
        public int poll(FragmentHandler fragmentHandler) {
            return 0;
        }
    }
}
