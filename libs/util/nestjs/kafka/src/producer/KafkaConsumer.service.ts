import {
  DecodedKafkaMessageV2,
  IKafkaMessageSerializerV2,
  KafkaMessage,
} from "@amplication/util/kafka";
import { Injectable } from "@nestjs/common";
import { KafkaMessageSchemaSerializer } from "../KafkaMessageSerializer";
import { BaseEventSchema } from "@amplication/schema-registry";

export const KAFKA_PRODUCER_SERVICE_NAME = "KAFKA_PRODUCER_SERVICE";

@Injectable()
export class KafkaConsumerService {
  constructor(private readonly serializerV2: IKafkaMessageSerializerV2) {
    this.serializerV2 = new KafkaMessageSchemaSerializer();
  }

  async decodeMessage<S extends BaseEventSchema>(
    message: KafkaMessage,
    schema: S
  ): Promise<DecodedKafkaMessageV2<S["key"], S["value"]>> {
    const decodedMessage = await this.serializerV2.deserialize(message, schema);
    return decodedMessage;
  }
}
