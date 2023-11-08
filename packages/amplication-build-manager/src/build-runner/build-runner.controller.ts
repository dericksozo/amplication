import { KafkaProducerService } from "@amplication/util/nestjs/kafka";
import { AmplicationLogger } from "@amplication/util/nestjs/logging";
import { Controller, Post } from "@nestjs/common";
import { ConfigService } from "@nestjs/config";
import { EventPattern, Payload } from "@nestjs/microservices";
import { Env } from "../env";
import { BuildRunnerService } from "./build-runner.service";
import { CodeGenerationFailureDto } from "./dto/CodeGenerationFailure";
import { CodeGenerationSuccessDto } from "./dto/CodeGenerationSuccess";
import {
  CodeGenerationFailure,
  CodeGenerationRequest,
  CodeGenerationSuccess,
  KAFKA_TOPICS,
} from "@amplication/schema-registry";
import { CodeGeneratorService } from "../code-generator/code-generator-catalog.service";
import { CodeGeneratorSplitterService } from "../code-generator/code-generator-splitter.service";
import { EnumEventStatus } from "../types";

@Controller("build-runner")
export class BuildRunnerController {
  constructor(
    private readonly configService: ConfigService<Env, true>,
    private readonly buildRunnerService: BuildRunnerService,
    private readonly codeGeneratorService: CodeGeneratorService,
    private readonly producerService: KafkaProducerService,
    private readonly logger: AmplicationLogger,
    private readonly codeGeneratorSplitterService: CodeGeneratorSplitterService
  ) {}

  @Post("code-generation-success")
  async onCodeGenerationSuccess(
    @Payload() dto: CodeGenerationSuccessDto
  ): Promise<void> {
    const buildId = this.codeGeneratorSplitterService.extractBuildId(
      dto.buildId
    );
    const codeGeneratorVersion =
      await this.buildRunnerService.getCodeGeneratorVersion(dto.buildId);

    try {
      await this.emitKafkaEventBasedOnJobStatus(
        dto.resourceId,
        dto.buildId,
        codeGeneratorVersion
      );
    } catch (error) {
      this.logger.error(error.message, error);

      const failureEvent: CodeGenerationFailure.KafkaEvent = {
        key: null,
        value: {
          buildId,
          error,
          codeGeneratorVersion,
        },
      };

      await this.producerService.emitMessage(
        KAFKA_TOPICS.CODE_GENERATION_FAILURE_TOPIC,
        failureEvent
      );
    }
  }

  @Post("code-generation-failure")
  async onCodeGenerationFailure(
    @Payload() dto: CodeGenerationFailureDto
  ): Promise<void> {
    const buildId = this.codeGeneratorSplitterService.extractBuildId(
      dto.buildId
    );
    try {
      const codeGeneratorVersion =
        await this.buildRunnerService.getCodeGeneratorVersion(dto.buildId);

      const failureEvent: CodeGenerationFailure.KafkaEvent = {
        key: null,
        value: { buildId, error: dto.error, codeGeneratorVersion },
      };

      await this.producerService.emitMessage(
        KAFKA_TOPICS.CODE_GENERATION_FAILURE_TOPIC,
        failureEvent
      );
    } catch (error) {
      this.logger.error(error.message, error);
    }
  }

  @EventPattern(KAFKA_TOPICS.CODE_GENERATION_REQUEST_TOPIC)
  async onCodeGenerationRequest(
    @Payload() message: CodeGenerationRequest.Value
  ): Promise<void> {
    this.logger.info("Code generation request received", {
      buildId: message.buildId,
      resourceId: message.resourceId,
    });

    let containerImageTag: string;
    try {
      if (this.configService.get(Env.DSG_CATALOG_SERVICE_URL)) {
        containerImageTag =
          await this.codeGeneratorService.getCodeGeneratorVersion({
            codeGeneratorVersion:
              message.dsgResourceData.resourceInfo.codeGeneratorVersionOptions
                .codeGeneratorVersion,
            codeGeneratorStrategy:
              message.dsgResourceData.resourceInfo.codeGeneratorVersionOptions
                .codeGeneratorStrategy,
          });
      }

      await this.buildRunnerService.runJobs(
        message.resourceId,
        message.buildId,
        message.dsgResourceData,
        containerImageTag ?? null
      );
    } catch (error) {
      this.logger.error(error.message, error);
      const failureEvent: CodeGenerationFailure.KafkaEvent = {
        key: null,
        value: {
          buildId: message.buildId,
          error,
          codeGeneratorVersion: containerImageTag ?? null,
        },
      };

      await this.producerService.emitMessage(
        KAFKA_TOPICS.CODE_GENERATION_FAILURE_TOPIC,
        failureEvent
      );
    }
  }

  /**
   * Emits a kafka event based on the job status (success / failure) from the redis cache
   * @param resourceId the resource id
   * @param buildId the original buildId without the suffix (domain name)
   * @param codeGeneratorVersion the code generator version
   */
  private async emitKafkaEventBasedOnJobStatus(
    resourceId: string,
    buildIdWithDomainName: string,
    codeGeneratorVersion: string
  ) {
    const buildId = this.codeGeneratorSplitterService.extractBuildId(
      buildIdWithDomainName
    );
    const [domainName, isSuccess] =
      await this.buildRunnerService.copyFromJobToArtifact(
        resourceId,
        buildIdWithDomainName
      );

    await this.codeGeneratorSplitterService.setJobStatusBasedOnArtifact(
      domainName,
      isSuccess,
      buildId
    );

    const jobStatus = await this.codeGeneratorSplitterService.getJobStatus(
      buildId
    );

    if (jobStatus === EnumEventStatus.Success) {
      const successEvent: CodeGenerationSuccess.KafkaEvent = {
        key: null,
        value: { buildId, codeGeneratorVersion },
      };

      await this.producerService.emitMessage(
        KAFKA_TOPICS.CODE_GENERATION_SUCCESS_TOPIC,
        successEvent
      );
    } else {
      const failureEvent: CodeGenerationFailure.KafkaEvent = {
        key: null,
        value: {
          buildId,
          error: new Error(`Code generation failed for ${domainName}`),
          codeGeneratorVersion,
        },
      };

      await this.producerService.emitMessage(
        KAFKA_TOPICS.CODE_GENERATION_FAILURE_TOPIC,
        failureEvent
      );
    }
  }
}
