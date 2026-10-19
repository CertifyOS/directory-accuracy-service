package com.certifyos.vendor_exchange;

import com.certifyos.vendor_exchange.auth.RequiresPermission;
import com.certifyos.vendor_exchange.config.MongoIndexes;
import com.certifyos.vendor_exchange.persistence.Collections;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tngtech.archunit.base.DescribedPredicate;
import com.tngtech.archunit.core.domain.JavaCall;
import com.tngtech.archunit.core.domain.properties.CanBeAnnotated;
import com.tngtech.archunit.core.domain.properties.HasName;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;
import com.tngtech.archunit.lang.CompositeArchRule;
import com.tngtech.archunit.lang.syntax.ArchRuleDefinition;
import com.tngtech.archunit.library.dependencies.SlicesRuleDefinition;
import jakarta.inject.Inject;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.PATCH;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import java.util.concurrent.Executors;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

/**
 * The module layout and coding rules from the new-service playbook, enforced on every build. Rules
 * that match no class yet use {@code allowEmptyShould(true)} so they are live from the first commit
 * and start biting the moment a class appears.
 */
@AnalyzeClasses(
        packages = "com.certifyos.vendor_exchange",
        importOptions = {ImportOption.DoNotIncludeTests.class, ImportOption.DoNotIncludeJars.class})
class ArchitectureTest {

    /** Shared code never imports feature code: the foundations know nothing about the export lane. */
    @ArchTest
    static final ArchRule sharedPackagesDoNotDependOnFeatures = ArchRuleDefinition.noClasses()
            .that()
            .resideInAnyPackage(
                    "..vendor_exchange.config..",
                    "..vendor_exchange.persistence..",
                    "..vendor_exchange.audit..",
                    "..vendor_exchange.auth..",
                    "..vendor_exchange.http..",
                    "..vendor_exchange.clients..",
                    "..vendor_exchange.metrics..")
            .should()
            .dependOnClassesThat()
            .resideInAPackage("..vendor_exchange.export..")
            .allowEmptyShould(true);

    /** No package cycles between the top-level slices. */
    @ArchTest
    static final ArchRule noPackageCycles = SlicesRuleDefinition.slices()
            .matching("com.certifyos.vendor_exchange.(*)..")
            .should()
            .beFreeOfCycles()
            .allowEmptyShould(true);

    /** Constructor injection only. A single constructor is injected by Quarkus without any annotation. */
    @ArchTest
    static final ArchRule noFieldInjection =
            ArchRuleDefinition.noFields().should().beAnnotatedWith(Inject.class).allowEmptyShould(true);

    /** One logger API: JBoss Logging, which Quarkus ships. */
    @ArchTest
    static final ArchRule oneLoggerApi = ArchRuleDefinition.noClasses()
            .should()
            .dependOnClassesThat()
            .resideInAnyPackage("org.slf4j..", "java.util.logging..", "org.apache.logging..")
            .allowEmptyShould(true);

    /** One injected ObjectMapper, never a private instance with its own settings. */
    @ArchTest
    static final ArchRule oneObjectMapper = ArchRuleDefinition.noClasses()
            .should()
            .callConstructor(ObjectMapper.class)
            .allowEmptyShould(true);

    /** Managed executors only; no ad hoc thread pools. */
    @ArchTest
    static final ArchRule noAdHocExecutors = ArchRuleDefinition.noClasses()
            .should()
            .callMethodWhere(com.tngtech.archunit.base.DescribedPredicate.describe(
                    "a factory on java.util.concurrent.Executors",
                    call -> call.getTargetOwner().isEquivalentTo(Executors.class)))
            .allowEmptyShould(true);

    /** No sleep-based waiting in production code. */
    @ArchTest
    static final ArchRule noThreadSleep = ArchRuleDefinition.noClasses()
            .should()
            .callMethod(Thread.class, "sleep", long.class)
            .allowEmptyShould(true);

    /** Contracts are typed: no JsonNode or raw Map in resource, event or client classes. */
    @ArchTest
    static final ArchRule typedContracts = ArchRuleDefinition.noClasses()
            .that()
            .resideInAnyPackage("..api..", "..events..", "..clients..")
            .should()
            .dependOnClassesThat()
            .haveFullyQualifiedName("com.fasterxml.jackson.databind.JsonNode")
            .allowEmptyShould(true);

    /** A method that answers an HTTP verb. */
    static final DescribedPredicate<CanBeAnnotated> HTTP_ENDPOINT = CanBeAnnotated.Predicates.annotatedWith(GET.class)
            .or(CanBeAnnotated.Predicates.annotatedWith(POST.class))
            .or(CanBeAnnotated.Predicates.annotatedWith(PUT.class))
            .or(CanBeAnnotated.Predicates.annotatedWith(PATCH.class))
            .or(CanBeAnnotated.Predicates.annotatedWith(DELETE.class))
            .as("an HTTP endpoint method");

    /**
     * Every served HTTP endpoint is documented: the committed OpenAPI contract is built from these.
     * REST client interfaces are callers, not endpoints.
     */
    @ArchTest
    static final ArchRule endpointsAreDocumented = ArchRuleDefinition.methods()
            .that()
            .areDeclaredInClassesThat()
            .areNotAnnotatedWith(RegisterRestClient.class)
            .and(HTTP_ENDPOINT)
            .should()
            .beAnnotatedWith(Operation.class)
            .allowEmptyShould(true);

    /** Every operator endpoint names the permission it needs, even while enforcement is off. */
    @ArchTest
    static final ArchRule operatorEndpointsDeclarePermission = ArchRuleDefinition.methods()
            .that()
            .areDeclaredInClassesThat()
            .resideInAPackage("..export.api..")
            .and()
            .areAnnotatedWith(Operation.class)
            .should()
            .beAnnotatedWith(RequiresPermission.class)
            .allowEmptyShould(true);

    /** The MongoCollection methods that change or remove documents. */
    static final String MONGO_WRITES = String.join(
            "|",
            "updateOne",
            "updateMany",
            "replaceOne",
            "deleteOne",
            "deleteMany",
            "findOneAndUpdate",
            "findOneAndReplace",
            "findOneAndDelete",
            "drop",
            "bulkWrite");

    /**
     * The two audit collections, {@code vendor_export_events} ({@code events()}) and {@code
     * vendor_review_events} ({@code reviewEvents()}), are append-only (designs, Audit trail): the
     * repository may insert and read, never update, replace or delete. Two halves: the audit
     * package calls no write method, and no class outside it reaches either collection, except
     * the accessor class itself and the index setup. Retention is a platform matter; nothing in
     * code shortens it.
     */
    @ArchTest
    static final ArchRule auditIsAppendOnly = CompositeArchRule.of(ArchRuleDefinition.noClasses()
                    .that()
                    .resideInAPackage("..audit..")
                    .should()
                    .callMethodWhere(JavaCall.Predicates.target(HasName.Predicates.nameMatching(MONGO_WRITES)))
                    .allowEmptyShould(true))
            .and(ArchRuleDefinition.noClasses()
                    .that()
                    .resideOutsideOfPackage("..audit..")
                    .and()
                    .doNotBelongToAnyOf(Collections.class, MongoIndexes.class)
                    .should()
                    .callMethod(Collections.class, "events")
                    .orShould()
                    .callMethod(Collections.class, "reviewEvents")
                    .allowEmptyShould(true));
}
