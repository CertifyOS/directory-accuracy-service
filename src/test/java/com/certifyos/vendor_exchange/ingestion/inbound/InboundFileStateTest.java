package com.certifyos.vendor_exchange.ingestion.inbound;

import static com.certifyos.vendor_exchange.ingestion.inbound.InboundFileState.ARCHIVING;
import static com.certifyos.vendor_exchange.ingestion.inbound.InboundFileState.DUPLICATE_LOGGED;
import static com.certifyos.vendor_exchange.ingestion.inbound.InboundFileState.FAILED;
import static com.certifyos.vendor_exchange.ingestion.inbound.InboundFileState.INGESTION_FAILED;
import static com.certifyos.vendor_exchange.ingestion.inbound.InboundFileState.INGESTION_REQUESTED;
import static com.certifyos.vendor_exchange.ingestion.inbound.InboundFileState.PARSED;
import static com.certifyos.vendor_exchange.ingestion.inbound.InboundFileState.RECEIVED;
import static com.certifyos.vendor_exchange.ingestion.inbound.InboundFileState.STAGED;
import static com.certifyos.vendor_exchange.ingestion.inbound.InboundFileState.STAGING;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class InboundFileStateTest {

    @Test
    void edgesMatchTheDesign() {
        Map<InboundFileState, Set<InboundFileState>> expected = Map.of(
                RECEIVED, EnumSet.of(ARCHIVING, FAILED),
                ARCHIVING, EnumSet.of(INGESTION_REQUESTED, FAILED),
                INGESTION_REQUESTED, EnumSet.of(PARSED, INGESTION_FAILED, FAILED),
                PARSED, EnumSet.of(STAGING, FAILED),
                STAGING, EnumSet.of(STAGED, FAILED),
                FAILED, EnumSet.of(RECEIVED, PARSED),
                STAGED, EnumSet.noneOf(InboundFileState.class),
                DUPLICATE_LOGGED, EnumSet.noneOf(InboundFileState.class),
                INGESTION_FAILED, EnumSet.noneOf(InboundFileState.class));

        for (InboundFileState from : InboundFileState.values()) {
            for (InboundFileState to : InboundFileState.values()) {
                Assertions.assertEquals(expected.get(from).contains(to), from.canTransitionTo(to), from + " -> " + to);
            }
        }
    }

    @Test
    void terminalAndReconcilableStates() {
        Set<InboundFileState> terminal = EnumSet.copyOf(Arrays.stream(InboundFileState.values())
                .filter(InboundFileState::isTerminal)
                .toList());

        Assertions.assertEquals(EnumSet.of(STAGED, DUPLICATE_LOGGED, INGESTION_FAILED), terminal);
        Assertions.assertEquals(EnumSet.of(RECEIVED, ARCHIVING, PARSED, STAGING), InboundFileState.reconcilable());
    }
}
