import { BaseEventSchema } from "@amplication/schema-registry";

export interface EmitArgs {
  topic: string;
  key: unknown;
  value: unknown;
  schema: BaseEventSchema;
  headers?: Record<string, string>;
}
