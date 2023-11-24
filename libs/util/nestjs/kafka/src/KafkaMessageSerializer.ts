import { BaseEventSchema, EventSchema } from "@amplication/schema-registry";
import { InferType, ValidationError } from "yup";
import { deserialize, serialize } from "v8";
import {
  DecodedKafkaMessageV2,
  IKafkaMessageSerializerV2,
  KafkaMessage,
} from "@amplication/util/kafka";

export class KafkaMessageSchemaSerializer implements IKafkaMessageSerializerV2 {
  private async validate<S extends BaseEventSchema>(event: {
    key: unknown;
    value: unknown;
    schema: EventSchema<S>;
  }): Promise<void> {
    const validationErrors: Error[] = [];
    try {
      await event.schema.key.validate(event.key, { strict: true });
    } catch (error) {
      if (error instanceof ValidationError) {
        validationErrors.push(error);
      }
    }
    try {
      await event.schema.value.validate(event.value, { strict: true });
    } catch (error) {
      if (error instanceof ValidationError) {
        validationErrors.push(error);
      }
    }
    if (validationErrors.length > 0) {
      throw new Error("Kafka message cannot be emitted as it is not valid", {
        cause: {
          errors: validationErrors,
        },
      });
    }
  }

  async deserialize<S extends BaseEventSchema>(
    message: KafkaMessage,
    schema: S
  ): Promise<
    DecodedKafkaMessageV2<
      InferType<typeof schema.key>,
      InferType<typeof schema.value>
    >
  > {
    const key = message.key ? deserialize(message.key) : null;
    const value = message.value ? deserialize(message.value) : null;

    await this.validate({
      key,
      value,
      schema,
    });

    return {
      key,
      value,
      headers: message.headers,
    };
  }
  async serialize<S extends BaseEventSchema>(
    message: DecodedKafkaMessageV2<unknown, unknown>,
    schema: EventSchema<S>
  ): Promise<KafkaMessage> {
    await this.validate({
      key: message.key,
      value: message.value,
      schema,
    });

    return {
      key: serialize(message.key),
      value: serialize(message.value),
      headers: message.headers,
    };
  }
}
