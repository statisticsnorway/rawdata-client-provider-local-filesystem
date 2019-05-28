package no.ssb.rawdata.provider.filesystem;

import io.reactivex.Flowable;
import no.ssb.config.DynamicConfiguration;
import no.ssb.rawdata.api.persistence.PersistenceQueue;
import no.ssb.rawdata.api.state.CompletedPosition;
import no.ssb.rawdata.api.state.StatePersistence;
import no.ssb.rawdata.api.state.StatePersistenceInitializer;
import no.ssb.rawdata.api.storage.RawdataClient;
import no.ssb.service.provider.api.ProviderConfigurator;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class FileSystemRawdataClient implements RawdataClient<CompletedPosition> {
    private final DynamicConfiguration configuration;
    private final String directory;
    private final PersistenceQueue<CompletedPosition> persistenceQueue;
    private final StatePersistence statePersistence;
    private Scheduler scheduler;

    public FileSystemRawdataClient(DynamicConfiguration configuration) {
        this.configuration = configuration;
        this.directory = configuration.evaluateToString("file.system.provider.directory");
        persistenceQueue = new PersistenceQueue<>(configuration);
        this.statePersistence = ProviderConfigurator.configure(configuration, configuration.evaluateToString("state.provider"), StatePersistenceInitializer.class);
    }

    @Override
    public byte[] read(String namespace, String position, String filename) {
        Path absoluteFilePath = Paths.get(String.format("%s/%s/%s", directory, namespace, position));

        try {
            return Files.readAllBytes(absoluteFilePath.resolve(filename));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void write(String namespace, String position, String filename, byte[] data) {
        Path absoluteFilePath = Paths.get(String.format("%s/%s/%s", directory, namespace, position));

        if (!Files.exists(absoluteFilePath)) {
            try {
                Files.createDirectories(absoluteFilePath);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        try {
            try (BufferedWriter writer = Files.newBufferedWriter(absoluteFilePath.resolve(filename), StandardCharsets.UTF_8)) {
                writer.write(new String(data));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Set<String> list(String namespace, String fromPosition, String toPosition) {
        Set<String> positionSet = new LinkedHashSet<>();
        statePersistence.readPositions(namespace, fromPosition, toPosition)
                .subscribe(onNext -> positionSet.add(onNext.position), onError -> {
                    throw new RuntimeException(onError);
                });
        return positionSet;
    }

    @Override
    public String firstPosition(String namespace) {
        return statePersistence.getFirstPosition(namespace).blockingGet();
    }

    @Override
    public String lastPosition(String namespace) {
        return statePersistence.getLastPosition(namespace).blockingGet();
    }

    @Override
    public String nextPosition(String namespace) {
        return statePersistence.getNextPosition(namespace).blockingGet();
    }

    @Override
    public String offsetPosition(String namespace, String fromPosition, int offset) {
        return statePersistence.getOffsetPosition(namespace, fromPosition, offset).blockingGet();
    }

    @Override
    public void publish(String namespace, Set<String> completedPositions) {
        statePersistence.trackCompletedPositions(namespace, completedPositions).blockingGet();
    }

    @Override
    public Flowable<CompletedPosition> subscription(String namespace, String fromPosition) {
        if (scheduler == null) {
            scheduler = new Scheduler(persistenceQueue, statePersistence, namespace, fromPosition);
        }
        return persistenceQueue.toFlowable();
    }

    @Override
    public void close() throws IOException {
        persistenceQueue.close();
    }

    static class Scheduler {
        private final ExecutorService executorService = Executors.newFixedThreadPool(1);
        private final AtomicBoolean handledFirstElement = new AtomicBoolean();
        private final AtomicReference<String> fromPosition = new AtomicReference<>();

        public Scheduler(PersistenceQueue<CompletedPosition> persistenceQueue, StatePersistence statePersistence, String namespace, String fromPosition) {
            this.fromPosition.set(fromPosition);
            executorService.submit(new PollPositionRunner(executorService, persistenceQueue, statePersistence, namespace, this.handledFirstElement, this.fromPosition));
        }
    }

    static class PollPositionRunner implements Runnable {
        private final ExecutorService executorService;
        private final PersistenceQueue<CompletedPosition> persistenceQueue;
        private final StatePersistence statePersistence;
        private final String namespace;
        private final AtomicBoolean handledFirstElement;  // assignment from Scheduler
        private final AtomicReference<String> fromPosition; // assignment from Scheduler

        public PollPositionRunner(ExecutorService executorService, PersistenceQueue<CompletedPosition> persistenceQueue, StatePersistence statePersistence, String namespace, AtomicBoolean handledFirstElement, AtomicReference<String> fromPosition) {
            this.executorService = executorService;
            this.persistenceQueue = persistenceQueue;
            this.statePersistence = statePersistence;
            this.namespace = namespace;
            this.handledFirstElement = handledFirstElement;
            this.fromPosition = fromPosition;
        }

        @Override
        public void run() {
            String toPosition = statePersistence.getLastPosition(namespace).blockingGet();
            Flowable<CompletedPosition> flowablePositions = statePersistence.readPositions(namespace, this.fromPosition.get(), toPosition);

            flowablePositions.subscribe(onNext ->                    {
                        if (!handledFirstElement.get() || !onNext.position.equals(fromPosition.get())) {
                            if (!handledFirstElement.get()) handledFirstElement.set(true);
                            this.fromPosition.set(onNext.position);
                            persistenceQueue.enqueue(onNext);
                        }
                    }, onError -> onError.printStackTrace(),
                    () -> {
                        TimeUnit.MILLISECONDS.sleep(250);
                        executorService.submit(new PollPositionRunner(executorService, persistenceQueue, statePersistence, namespace, this.handledFirstElement, this.fromPosition));
                    });

        }
    }
}
