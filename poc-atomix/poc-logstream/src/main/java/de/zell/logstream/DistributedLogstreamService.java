package de.zell.logstream;

import io.atomix.primitive.operation.Command;
import org.agrona.DirectBuffer;

public interface DistributedLogstreamService {
    @Command
    void append(DirectBuffer bytes);
}
