package raftkv.server;

import raftkv.raft.RaftNode;
import raftkv.raft.Types.ClientResult;
import raftkv.raft.Types.Command;
import raftkv.raft.Types.Status;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/** Serializes every Raft callback for one replica onto a single thread. */
public final class NodeLoop implements AutoCloseable {
    private final RaftNode node;
    private final BlockingQueue<Runnable> inbox = new LinkedBlockingQueue<>();
    private final Thread thread;
    private volatile boolean running = true;
    private volatile Throwable lastError;

    public NodeLoop(RaftNode node) {
        this.node = node;
        this.thread = new Thread(this::loop, "raft-" + node.id());
    }

    public RaftNode node() {
        return node;
    }

    public Throwable lastError() {
        return lastError;
    }

    public void start() {
        thread.start();
    }

    public void submit(Runnable task) {
        if (!inbox.offer(task)) {
            throw new IllegalStateException("raft inbox closed");
        }
    }

    public <T> T call(Supplier<T> task, long timeoutMs) {
        CompletableFuture<T> future = new CompletableFuture<>();
        submit(() -> {
            try {
                future.complete(task.get());
            } catch (Throwable error) {
                future.completeExceptionally(error);
            }
        });
        return await(future, timeoutMs);
    }

    public ClientResult propose(Command command, long timeoutMs) {
        try {
            return await(proposeAsync(command), timeoutMs);
        } catch (IllegalStateException timeout) {
            return ClientResult.unknown(null);
        }
    }

    public CompletableFuture<ClientResult> proposeAsync(Command command) {
        CompletableFuture<ClientResult> future = new CompletableFuture<>();
        submit(() -> node.propose(command, future::complete));
        return future;
    }

    public ClientResult get(String key, long timeoutMs) {
        try {
            return await(getAsync(key), timeoutMs);
        } catch (IllegalStateException timeout) {
            return ClientResult.unknown(null);
        }
    }

    public CompletableFuture<ClientResult> getAsync(String key) {
        CompletableFuture<ClientResult> future = new CompletableFuture<>();
        submit(() -> node.linearizableGet(key, future::complete));
        return future;
    }

    public Status status(long timeoutMs) {
        return call(node::status, timeoutMs);
    }

    @Override
    public void close() {
        running = false;
        submit(node::crash);
        thread.interrupt();
        try {
            thread.join(2_000);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private void loop() {
        long nextTick = System.currentTimeMillis() + 10;
        while (running) {
            long wait = Math.max(1, nextTick - System.currentTimeMillis());
            try {
                Runnable task = inbox.poll(wait, TimeUnit.MILLISECONDS);
                if (task != null) {
                    task.run();
                }
            } catch (InterruptedException interrupted) {
                if (!running) {
                    return;
                }
            } catch (Throwable error) {
                lastError = error;
                error.printStackTrace(System.err);
            }
            long now = System.currentTimeMillis();
            if (now >= nextTick) {
                try {
                    node.onTick();
                } catch (Throwable error) {
                    lastError = error;
                    error.printStackTrace(System.err);
                }
                nextTick = now + 10;
            }
        }
    }

    private static <T> T await(CompletableFuture<T> future, long timeoutMs) {
        try {
            return future.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException timeout) {
            throw new IllegalStateException("timed out waiting for raft thread");
        } catch (Exception error) {
            throw new IllegalStateException(error);
        }
    }
}
