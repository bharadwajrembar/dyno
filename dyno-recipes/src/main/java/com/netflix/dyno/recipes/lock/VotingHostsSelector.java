package com.netflix.dyno.recipes.lock;

import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.impl.lb.CircularList;

public interface VotingHostsSelector {
    CircularList<Host> getVotingHosts();

    int getVotingSize();
}
