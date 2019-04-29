import no.ssb.rawdata.api.storage.RawdataClientInitializer;

module no.ssb.rawdata.storage.provider.filesystem {
    requires java.base;
    requires java.logging;
    requires org.slf4j;
    requires io.reactivex.rxjava2;
    requires no.ssb.config;
    requires no.ssb.rawdata.api;
    requires no.ssb.service.provider.api;

    provides RawdataClientInitializer with no.ssb.rawdata.provider.filesystem.FileSystemRawdataClientInitializer;

    exports no.ssb.rawdata.provider.filesystem;
}
