/*
 * Copyright (c) 2021 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.sync;

import com.google.common.annotations.VisibleForTesting;
import io.airbyte.commons.functional.CheckedSupplier;
import io.airbyte.commons.io.LineGobbler;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.logging.LoggingHelper;
import io.airbyte.commons.logging.MdcScope;
import io.airbyte.config.AirbyteConfigValidator;
import io.airbyte.config.ConfigSchema;
import io.airbyte.config.Configs.WorkerEnvironment;
import io.airbyte.config.ReplicationOutput;
import io.airbyte.config.StandardSyncInput;
import io.airbyte.config.StandardSyncOutput;
import io.airbyte.config.StandardSyncSummary;
import io.airbyte.config.helpers.LogConfigs;
import io.airbyte.config.persistence.split_secrets.SecretsHydrator;
import io.airbyte.scheduler.models.IntegrationLauncherConfig;
import io.airbyte.scheduler.models.JobRunConfig;
import io.airbyte.workers.Worker;
import io.airbyte.workers.WorkerApp;
import io.airbyte.workers.WorkerConfigs;
import io.airbyte.workers.WorkerException;
import io.airbyte.workers.WorkerUtils;
import io.airbyte.workers.process.KubeProcessFactory;
import io.airbyte.workers.process.ProcessFactory;
import io.airbyte.workers.temporal.CancellationHandler;
import io.airbyte.workers.temporal.TemporalAttemptExecution;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicationActivityImpl implements ReplicationActivity {

  public static final String REPLICATION = "replication";
  public static final String INIT_FILE_APPLICATION = "application.txt";
  public static final String INIT_FILE_JOB_RUN_CONFIG = "jobRunConfig.json";
  public static final String INIT_FILE_SOURCE_LAUNCHER_CONFIG = "sourceLauncherConfig.json";
  public static final String INIT_FILE_DESTINATION_LAUNCHER_CONFIG = "destinationLauncherConfig.json";
  public static final String INIT_FILE_SYNC_INPUT = "syncInput.json";
  public static final String INIT_FILE_ENV_MAP = "envMap.json";

  private static final Logger LOGGER = LoggerFactory.getLogger(ReplicationActivityImpl.class);

  private final WorkerConfigs workerConfigs;
  private final ProcessFactory processFactory;
  private final SecretsHydrator secretsHydrator;
  private final Path workspaceRoot;
  private final AirbyteConfigValidator validator;
  private final WorkerEnvironment workerEnvironment;
  private final LogConfigs logConfigs;

  private final String databaseUser;
  private final String databasePassword;
  private final String databaseUrl;
  private final String airbyteVersion;

  private static final MdcScope.Builder LOG_MDC_BUILDER = new MdcScope.Builder()
      .setLogPrefix("container-orchestrator")
      .setPrefixColor(LoggingHelper.Color.CYAN_BACKGROUND);

  public ReplicationActivityImpl(final WorkerConfigs workerConfigs,
                                 final ProcessFactory processFactory,
                                 final SecretsHydrator secretsHydrator,
                                 final Path workspaceRoot,
                                 final WorkerEnvironment workerEnvironment,
                                 final LogConfigs logConfigs,
                                 final String databaseUser,
                                 final String databasePassword,
                                 final String databaseUrl,
                                 final String airbyteVersion) {
    this(workerConfigs, processFactory, secretsHydrator, workspaceRoot, workerEnvironment, logConfigs, new AirbyteConfigValidator(), databaseUser,
        databasePassword, databaseUrl, airbyteVersion);
  }

  @VisibleForTesting
  ReplicationActivityImpl(final WorkerConfigs workerConfigs,
                          final ProcessFactory processFactory,
                          final SecretsHydrator secretsHydrator,
                          final Path workspaceRoot,
                          final WorkerEnvironment workerEnvironment,
                          final LogConfigs logConfigs,
                          final AirbyteConfigValidator validator,
                          final String databaseUser,
                          final String databasePassword,
                          final String databaseUrl,
                          final String airbyteVersion) {
    this.workerConfigs = workerConfigs;
    this.processFactory = processFactory;
    this.secretsHydrator = secretsHydrator;
    this.workspaceRoot = workspaceRoot;
    this.validator = validator;
    this.workerEnvironment = workerEnvironment;
    this.logConfigs = logConfigs;
    this.databaseUser = databaseUser;
    this.databasePassword = databasePassword;
    this.databaseUrl = databaseUrl;
    this.airbyteVersion = airbyteVersion;
  }

  @Override
  public StandardSyncOutput replicate(final JobRunConfig jobRunConfig,
                                      final IntegrationLauncherConfig sourceLauncherConfig,
                                      final IntegrationLauncherConfig destinationLauncherConfig,
                                      final StandardSyncInput syncInput) {

    final var fullSourceConfig = secretsHydrator.hydrate(syncInput.getSourceConfiguration());
    final var fullDestinationConfig = secretsHydrator.hydrate(syncInput.getDestinationConfiguration());

    final var fullSyncInput = Jsons.clone(syncInput)
        .withSourceConfiguration(fullSourceConfig)
        .withDestinationConfiguration(fullDestinationConfig);

    final Supplier<StandardSyncInput> inputSupplier = () -> {
      validator.ensureAsRuntime(ConfigSchema.STANDARD_SYNC_INPUT, Jsons.jsonNode(fullSyncInput));
      return fullSyncInput;
    };

    final TemporalAttemptExecution<StandardSyncInput, ReplicationOutput> temporalAttempt = new TemporalAttemptExecution<>(
        workspaceRoot,
        workerEnvironment,
        logConfigs,
        jobRunConfig,
        getWorkerFactory(sourceLauncherConfig, destinationLauncherConfig, jobRunConfig, syncInput),
        inputSupplier,
        new CancellationHandler.TemporalCancellationHandler(),
        databaseUser,
        databasePassword,
        databaseUrl,
        airbyteVersion);

    final ReplicationOutput attemptOutput = temporalAttempt.get();
    final StandardSyncOutput standardSyncOutput = reduceReplicationOutput(attemptOutput);

    LOGGER.info("sync summary: {}", standardSyncOutput);

    return standardSyncOutput;
  }

  private static StandardSyncOutput reduceReplicationOutput(final ReplicationOutput output) {
    final long totalBytesReplicated = output.getReplicationAttemptSummary().getBytesSynced();
    final long totalRecordsReplicated = output.getReplicationAttemptSummary().getRecordsSynced();

    final StandardSyncSummary syncSummary = new StandardSyncSummary();
    syncSummary.setBytesSynced(totalBytesReplicated);
    syncSummary.setRecordsSynced(totalRecordsReplicated);
    syncSummary.setStartTime(output.getReplicationAttemptSummary().getStartTime());
    syncSummary.setEndTime(output.getReplicationAttemptSummary().getEndTime());
    syncSummary.setStatus(output.getReplicationAttemptSummary().getStatus());

    final StandardSyncOutput standardSyncOutput = new StandardSyncOutput();
    standardSyncOutput.setState(output.getState());
    standardSyncOutput.setOutputCatalog(output.getOutputCatalog());
    standardSyncOutput.setStandardSyncSummary(syncSummary);

    return standardSyncOutput;
  }

  private CheckedSupplier<Worker<StandardSyncInput, ReplicationOutput>, Exception> getWorkerFactory(
                                                                                                    final IntegrationLauncherConfig sourceLauncherConfig,
                                                                                                    final IntegrationLauncherConfig destinationLauncherConfig,
                                                                                                    final JobRunConfig jobRunConfig,
                                                                                                    final StandardSyncInput syncInput) {
    return () -> new Worker<>() {

      final AtomicBoolean cancelled = new AtomicBoolean(false);
      Process process;

      @Override
      public ReplicationOutput run(StandardSyncInput standardSyncInput, Path jobRoot) throws WorkerException {
        try {
          final Path jobPath = WorkerUtils.getJobRoot(workspaceRoot, jobRunConfig.getJobId(), jobRunConfig.getAttemptId());

          final Map<String, String> fileMap = Map.of(
              INIT_FILE_APPLICATION, REPLICATION,
              INIT_FILE_JOB_RUN_CONFIG, Jsons.serialize(jobRunConfig),
              INIT_FILE_SOURCE_LAUNCHER_CONFIG, Jsons.serialize(sourceLauncherConfig),
              INIT_FILE_DESTINATION_LAUNCHER_CONFIG, Jsons.serialize(destinationLauncherConfig),
              INIT_FILE_SYNC_INPUT, Jsons.serialize(syncInput),
              INIT_FILE_ENV_MAP, Jsons.serialize(System.getenv()));

          // for now keep same failure behavior where this is heartbeating and depends on the parent worker to
          // exist
          process = processFactory.create(
              "runner-" + UUID.randomUUID().toString().substring(0, 10),
              0,
              jobPath,
              "airbyte/container-orchestrator:" + airbyteVersion,
              false,
              fileMap,
              null,
              workerConfigs.getResourceRequirements(),
              Map.of(KubeProcessFactory.JOB_TYPE, KubeProcessFactory.SYNC_RUNNER),
              Map.of(WorkerApp.KUBE_HEARTBEAT_PORT, WorkerApp.KUBE_HEARTBEAT_PORT));

          final AtomicReference<ReplicationOutput> output = new AtomicReference<>();

          LineGobbler.gobble(process.getInputStream(), line -> {
            final Optional<ReplicationOutput> maybeOutput = Jsons.tryDeserialize(line, ReplicationOutput.class);

            if (maybeOutput.isPresent()) {
              LOGGER.info("Found output!");
              output.set(maybeOutput.get());
            } else {
              try (final var mdcScope = LOG_MDC_BUILDER.build()) {
                LOGGER.info(line);
              }
            }
          });

          LineGobbler.gobble(process.getErrorStream(), LOGGER::error, LOG_MDC_BUILDER);

          WorkerUtils.wait(process);

          if (process.exitValue() != 0) {
            throw new WorkerException("Non-zero exit code!");
          }

          if (output.get() != null) {
            return output.get();
          } else {
            throw new WorkerException("Running the sync attempt resulted in no readable output!");
          }
        } catch (Exception e) {
          if (cancelled.get()) {
            throw new WorkerException("Sync was cancelled.", e);
          } else {
            throw new WorkerException("Running the sync attempt failed", e);
          }
        }
      }

      @Override
      public void cancel() {
        cancelled.set(true);

        if (process == null) {
          return;
        }

        LOGGER.debug("Closing sync runner process");
        WorkerUtils.gentleClose(workerConfigs, process, 1, TimeUnit.MINUTES);
        if (process.isAlive() || process.exitValue() != 0) {
          LOGGER.error("Sync runner process wasn't successful");
        }
      }

    };
  }

}
