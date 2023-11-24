import {
  DecodedKafkaMessage,
  IKafkaMessageSerializer,
  IKafkaMessageSerializerV2,
  KAFKA_SERIALIZER,
  SchemaIds,
} from "@amplication/util/kafka";
import { Inject, Injectable } from "@nestjs/common";
import { ClientKafka } from "@nestjs/microservices";
import { KAFKA_CLIENT } from "../createNestjsKafkaConfig";
import { KafkaMessageSchemaSerializer } from "../KafkaMessageSerializer";
import { EmitArgs } from "./KafkaProducer.types";

export const KAFKA_PRODUCER_SERVICE_NAME = "KAFKA_PRODUCER_SERVICE";

@Injectable()
export class KafkaProducerService {
  constructor(
    @Inject(KAFKA_CLIENT)
    private readonly kafkaClient: ClientKafka,
    @Inject(KAFKA_SERIALIZER)
    private readonly serializer: IKafkaMessageSerializer,
    private readonly serializerV2: IKafkaMessageSerializerV2
  ) {
    this.serializerV2 = new KafkaMessageSchemaSerializer();
  }

  async emit(event: EmitArgs): Promise<void> {
    const { topic, schema, key, value, headers } = event;

    const kafkaMessage = this.serializerV2.serialize(
      {
        key,
        value,
        headers,
      },
      schema
    );

    return await new Promise((resolve, reject) => {
      this.kafkaClient.emit(topic, kafkaMessage).subscribe({
        error: (err: Error) => {
          reject(err);
        },
        next: () => {
          resolve();
        },
      });
    });
  }

  async emitMessage(
    topic: string,
    message: DecodedKafkaMessage,
    schemaIds?: SchemaIds
  ): Promise<void> {
    const kafkaMessage = await this.serializer.serialize(message, schemaIds);
    return await new Promise((resolve, reject) => {
      this.kafkaClient.emit(topic, kafkaMessage).subscribe({
        error: (err: Error) => {
          reject(err);
        },
        next: () => {
          resolve();
        },
      });
    });
  }
}
