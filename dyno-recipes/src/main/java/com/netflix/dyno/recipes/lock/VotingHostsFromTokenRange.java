package com.netflix.dyno.recipes.lock;

import com.google.common.collect.ImmutableSet;
import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.HostSupplier;
import com.netflix.dyno.connectionpool.TokenMapSupplier;
import com.netflix.dyno.connectionpool.impl.lb.CircularList;
import com.netflix.dyno.connectionpool.impl.lb.HostToken;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

public class VotingHostsFromTokenRange implements VotingHostsSelector {

    private final TokenMapSupplier tokenMapSupplier;
    private final HostSupplier hostSupplier;
    private final CircularList<Host> votingHosts = new CircularList<>(new ArrayList<>());
    // TODO: raghu Might be easier as a FP?
    private final int MIN_VOTING_SIZE = 1;
    private final int MAX_VOTING_SIZE = 5;
    private final int effectiveVotingSize;
    private final AtomicInteger calculatedVotingSize = new AtomicInteger(0);

    public VotingHostsFromTokenRange(HostSupplier hostSupplier, TokenMapSupplier tokenMapSupplier, int votingSize) {
        this.tokenMapSupplier = tokenMapSupplier;
        this.hostSupplier = hostSupplier;
        effectiveVotingSize = votingSize == -1 ? MAX_VOTING_SIZE : votingSize;
        getVotingHosts();
    }

    @Override
    public CircularList<Host> getVotingHosts() {
        if (votingHosts.getSize() == 0) {
            List<HostToken> allHostTokens = tokenMapSupplier.getTokens(ImmutableSet.copyOf(hostSupplier.getHosts()));
            if (allHostTokens.size() < MIN_VOTING_SIZE) {
                throw new IllegalStateException(String.format("Cannot perform voting with less than %d nodes", MIN_VOTING_SIZE));
            }
            Map<String, Long> numHostsPerRack = allHostTokens.stream().map(ht -> ht.getHost().getRack()).collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
            AtomicInteger numHostsRequired = new AtomicInteger(effectiveVotingSize);
            Map<String, Integer> numHosts = new HashMap<>();
            List<String> racks = numHostsPerRack.keySet().stream().sorted(Comparator.comparing(String::toString)).collect(Collectors.toList());
            for(String rack: racks) {
                if(numHostsRequired.get() <= 0) {
                    numHosts.put(rack, 0);
                    continue;
                }
                int v = (int) Math.min(numHostsRequired.get(), numHostsPerRack.get(rack));
                numHostsRequired.addAndGet(-v);
                numHosts.put(rack, v);
                calculatedVotingSize.addAndGet(v);
            }
            Map<String, List<HostToken>> rackToHostToken = allHostTokens.stream()
                    .collect(Collectors.groupingBy(ht -> ht.getHost().getRack()));
            allHostTokens.sort(HostToken::compareTo);
            List<Host> finalVotingHosts = rackToHostToken.entrySet().stream()
                    .sorted(Comparator.comparing(Map.Entry::getKey))
                    .flatMap(e -> {
                        List<HostToken> temp = e.getValue();
                        temp.sort(HostToken::compareTo);
                        return temp.subList(0, numHosts.get(e.getKey())).stream();
                    })
                    .map(ht -> ht.getHost())
                    .collect(Collectors.toList());
            votingHosts.swapWithList(finalVotingHosts);
        }
        return votingHosts;
    }

    @Override
    public int getVotingSize() {
        return calculatedVotingSize.get();
    }
}
