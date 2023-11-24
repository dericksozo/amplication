import { BaseEventSchema, EventSchema } from "@amplication/schema-registry";
import type {
  DecodedKafkaMessage,
  DecodedKafkaMessageV2,
  KafkaMessage,
  SchemaIds,
} from "./kafka.types";
import { InferType } from "yup";

export const KAFKA_SERIALIZER = "KAFKA_SERIALIZER";

export interface IKafkaMessageSerializer {
  serialize: (
    message: DecodedKafkaMessage,
    schemaIds?: SchemaIds
  ) => Promise<KafkaMessage>;

  deserialize: (
    message: KafkaMessage
  ) => Promise<DecodedKafkaMessage> | DecodedKafkaMessage;
}

export interface IKafkaMessageSerializerV2 {
  serialize: <S extends BaseEventSchema>(
    message: DecodedKafkaMessageV2<unknown, unknown>,
    schema: EventSchema<S>
  ) => Promise<KafkaMessage>;

  deserialize: <S extends BaseEventSchema>(
    message: KafkaMessage,
    schema: S
  ) =>
    | Promise<DecodedKafkaMessageV2<typeof schema.key, typeof schema.value>>
    | DecodedKafkaMessageV2<typeof schema.key, typeof schema.value>;
}
