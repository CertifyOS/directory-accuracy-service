package com.certifyos.directory_accuracy.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import java.time.Duration;
import java.util.Optional;

/**
 * Deployment configuration, one closed namespace: {@code vendor-exchange.*}. SmallRye validates the
 * whole mapping at startup, so a key without a default that the environment forgets fails the boot
 * instead of running with a wrong value. Keys that are {@link Optional} are the swap-in points from
 * the scaffolding plan (section 2): absent until the external dependency exists, read by code that
 * reports "not configured" rather than failing.
 */
@ConfigMapping(prefix = "vendor-exchange")
public interface VendorExchangeConfig {

    /** Service-wide kill switch: the tick inserts nothing, jobs exit as no-ops, events are acknowledged and ignored. */
    @WithDefault("true")
    boolean enabled();

    /** JobRunr retries per job, exponential backoff. Design doc {@code jobRetries}. */
    @WithDefault("8")
    int jobRetries();

    /** Age after which an untouched non-terminal batch is re-enqueued and alerted. Design doc {@code reconcilerStaleMinutes}. */
    @WithDefault("30")
    int reconcilerStaleMinutes();

    /** Bucket egress copies the finished file to and {@code FinishJob} verifies it in. Design doc {@code vendorBucket}. */
    String vendorBucket();

    /** The egress service (file build and placement). */
    Egress egress();

    /** The DAL, called through IAP for the user and tenant lookup. */
    Dal dal();

    /** api-layer, called with an Auth0 client-credentials token for practitioner selection and templates. */
    ApiLayer apiLayer();

    /** Ingestion of the vendor's response file (phase 2). */
    Ingestion ingestion();

    /** file-ingestion-service, called for the NDJSON stream of the mapped vendor file. */
    FileIngestion fileIngestion();

    /** Pub/Sub push delivery of the egress completion event. */
    PubSub pubsub();

    /** Operator permission enforcement. */
    Permissions permissions();

    /** User lookup cache. */
    Auth auth();

    /** Egress settings. */
    interface Egress {
        /** Base URL of the egress service. */
        String url();

        /** IAP OAuth client id of the egress backend, the audience of the Google ID token we send. */
        String iapClientId();

        /** When the first deadline check runs after the egress request. Design doc {@code egressDeadlineHours}. */
        @WithDefault("6")
        int deadlineHours();

        /** When the second check runs and a still-running job is failed. Design doc {@code egressAbandonHours}. */
        @WithDefault("48")
        int abandonHours();
    }

    /** DAL settings. */
    interface Dal {
        /** Base URL of the DAL. */
        String url();

        /** IAP OAuth client id of the DAL backend, the audience of the Google ID token we send. */
        String iapClientId();
    }

    /** api-layer settings. The client id and secret are absent until the Auth0 machine client exists. */
    interface ApiLayer {
        /** Base URL of api-layer. */
        String url();

        /** Auth0 machine-to-machine client id; absent means api-layer calls are not configured yet. */
        Optional<String> clientId();

        /** Auth0 machine-to-machine client secret; absent means api-layer calls are not configured yet. */
        Optional<String> clientSecret();

        /**
         * IAP audience of api-layer's backend when it sits behind IAP with a Google-managed client
         * ({@code /projects/<number>/global/backendServices/<id>}). Present: a Google ID token for
         * it is sent in {@code Proxy-Authorization} beside the Auth0 bearer. Absent: no IAP header.
         */
        Optional<String> iapAudience();
    }

    /** Ingestion settings. */
    interface Ingestion {
        /** Bucket the received vendor file is archived in before and after processing. */
        String archiveBucket();

        /** Hours ingestion of one inbound file may take before the deadline check applies. */
        @WithDefault("6")
        int ingestionDeadlineHours();

        /** Rows staged per write when the mapped file is read. */
        @WithDefault("500")
        int stageChunkRows();

        /** Largest page size a review list request may ask for. */
        @WithDefault("200")
        int reviewPageLimitMax();

        /** Months a rejected value is remembered so the same value is not proposed again. */
        @WithDefault("12")
        int rejectedValueTtlMonths();

        /** Minutes a release waits for the read back of the written values before it is alerted. */
        @WithDefault("30")
        int releaseReadBackDeadlineMinutes();

        /** Most proposals a single bulk release may cover. */
        @WithDefault("5000")
        int releaseBulkMax();

        /** Minutes without progress after which an inbound file counts as stale. */
        @WithDefault("30")
        int inboundStaleMinutes();

        /** Which signal tells ingestion that the vendor's delivery is complete. */
        @WithDefault("DATA_FILE")
        String completenessSignal();
    }

    /**
     * file-ingestion-service settings. One vendor in the MVP, so the design's per-vendor mapping id
     * is the single {@code mappingId}; a second vendor turns it into a map.
     */
    interface FileIngestion {
        /** Base URL of file-ingestion-service. */
        String url();

        /** Name this service identifies itself with to file-ingestion-service. */
        @WithDefault("vendor-ingestion")
        String consumerIdentifier();

        /** Mapping that turns the vendor file into rows; absent means ingestion is not configured yet. */
        Optional<String> mappingId();

        /**
         * IAP audience of file-ingestion-service's backend when it sits behind IAP with a
         * Google-managed client. Present: a Google ID token for it is sent. Absent: no IAP header.
         */
        Optional<String> iapAudience();
    }

    /** Pub/Sub push settings. Both absent until the subscription exists; the push endpoint answers 401 meanwhile. */
    interface PubSub {
        /** Service account Pub/Sub pushes with; the {@code email} claim the token must carry. */
        Optional<String> pushServiceAccount();

        /** Audience the push identity token must carry, normally the push endpoint URL. */
        Optional<String> pushAudience();
    }

    /** Permission settings. */
    interface Permissions {
        /**
         * When true, endpoints check the specific action ({@code vendor-export:manage} or {@code read}).
         * When false, tenant membership alone is enough, as in file-ingestion-service.
         */
        @WithDefault("false")
        boolean enforce();
    }

    /** Auth cache settings. */
    interface Auth {
        /** How long a user lookup is cached. */
        @WithDefault("PT5M")
        Duration cacheTtl();

        /** Maximum cached users. */
        @WithDefault("1000")
        int cacheMaxSize();
    }
}
