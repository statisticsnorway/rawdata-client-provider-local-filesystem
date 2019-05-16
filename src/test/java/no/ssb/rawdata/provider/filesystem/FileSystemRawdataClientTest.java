package no.ssb.rawdata.provider.filesystem;

import io.reactivex.Flowable;
import io.reactivex.schedulers.Schedulers;
import no.ssb.config.DynamicConfiguration;
import no.ssb.config.StoreBasedDynamicConfiguration;
import no.ssb.rawdata.api.state.CompletedPosition;
import no.ssb.rawdata.api.state.LinkedSet;
import no.ssb.rawdata.api.storage.RawdataClient;
import no.ssb.rawdata.api.storage.RawdataClientInitializer;
import no.ssb.service.provider.api.ProviderConfigurator;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class FileSystemRawdataClientTest {

    private DynamicConfiguration configuration;
    private RawdataClient rawdataClient;

    static DynamicConfiguration configuration() {
        Path currentPath = Paths.get("").toAbsolutePath().resolve("target");
        return new StoreBasedDynamicConfiguration.Builder()
                .propertiesResource("application-defaults.properties")
                .propertiesResource("application-test.properties")
                .values("state.provider", "h2")
                .values("database.h2.url", "jdbc:h2:/tmp/rawdata/h2db")
                .values("storage.provider", "fs")
                .values("file.system.provider.directory", currentPath.toString())
                .build();
    }

    static RawdataClient storageProvider(DynamicConfiguration configuration) {
        return ProviderConfigurator.configure(configuration, "fs", RawdataClientInitializer.class);
    }

    @BeforeClass
    public void setUp() {
        configuration = configuration();
        rawdataClient = storageProvider(configuration);
        assertNotNull(rawdataClient);
    }

    @Test
    public void thatWriteAndReadAreEqual() throws IOException, InterruptedException {
        Flowable<CompletedPosition> completedPositionFlowable = rawdataClient.subscription("ns", "1");

        byte[] rawdata = "foo".getBytes();
        rawdataClient.write("ns", "1", "file-1.txt", rawdata);
        byte[] readRawData = rawdataClient.read("ns", "1", "file-1.txt");
        assertEquals(rawdata, readRawData);

        rawdataClient.publish("ns", LinkedSet.of("1"));

        Set<String> positions = rawdataClient.list("ns", "1", "1");
        assertEquals(positions.size(), 1);

        completedPositionFlowable.subscribeOn(Schedulers.newThread()).observeOn(Schedulers.newThread())
                .subscribe(onNext -> {
                    System.out.printf("received: %s/%s%n", onNext.namespace, onNext.position);
                }, onError -> onError.printStackTrace());

        rawdataClient.publish("ns", LinkedSet.of("2"));

        positions = rawdataClient.list("ns", "1", "2");
        assertEquals(positions.size(), 2);

        Thread.sleep(1000);
    }

}
