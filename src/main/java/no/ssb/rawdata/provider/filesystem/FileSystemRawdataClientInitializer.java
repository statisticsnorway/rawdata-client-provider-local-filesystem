package no.ssb.rawdata.provider.filesystem;

import no.ssb.config.DynamicConfiguration;
import no.ssb.rawdata.api.storage.RawdataClient;
import no.ssb.rawdata.api.storage.RawdataClientInitializer;
import no.ssb.service.provider.api.ProviderName;

import java.util.HashSet;
import java.util.Set;

@ProviderName("fs")
public class FileSystemRawdataClientInitializer implements RawdataClientInitializer {

    @Override
    public String providerId() {
        return "fs";
    }

    @Override
    public Set<String> configurationKeys() {
        return new HashSet<>();
    }

    @Override
    public RawdataClient initialize(DynamicConfiguration configuration) {
        return new FileSystemRawdataClient(configuration);
    }
}
