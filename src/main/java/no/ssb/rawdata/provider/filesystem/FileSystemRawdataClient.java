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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class FileSystemRawdataClient implements RawdataClient {
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
    public String nextPosition(String namespace) {
        return statePersistence.getNextPosition(namespace).blockingGet();
    }

    @Override
    public void publishExpectedPositions(String namespace, Set<String> expectedPositions) {
        statePersistence.expectedPagePositions(namespace, expectedPositions).blockingGet();
    }

    @Override
    public void publishCompletedPositions(String namespace, Set<String> completedPositions) {
        statePersistence.trackCompletedPositions(namespace, completedPositions).blockingGet();
    }

    @Override
    public Flowable<CompletedPosition> subscribe(String namespace, String fromPosition) {
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
        private final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
        private final AtomicReference<String> fromPosition = new AtomicReference<>();

        public Scheduler(PersistenceQueue<CompletedPosition> persistenceQueue, StatePersistence statePersistence, String namespace, String fromPosition) {
            this.fromPosition.set(fromPosition);

            executorService.scheduleAtFixedRate(() -> {
                String toPosition = statePersistence.getLastPosition(namespace).blockingGet();
                Flowable<CompletedPosition> flowablePositions = statePersistence.readPositions(namespace, this.fromPosition.get(), toPosition);

                AtomicReference<CompletedPosition> completedPosition = new AtomicReference<>();
                flowablePositions.subscribe(onNext ->
                        {
                            completedPosition.set(onNext);
                            persistenceQueue.enqueue(onNext);
                        }, onError -> onError.printStackTrace(),
                        () -> {
                            if (completedPosition.get() != null) {
                                this.fromPosition.set(completedPosition.get().position);
                            }
                        });
            }, 0L, 1L, TimeUnit.SECONDS);
        }
    }
}
