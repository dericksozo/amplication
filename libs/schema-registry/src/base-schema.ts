import { object, InferType } from "yup";

export const BaseEventKey = object().optional().nullable();
export const BaseEventValue = object();

export type BaseEventSchema = {
  key: InferType<typeof BaseEventKey>;
  value: InferType<typeof BaseEventValue>;
};

export interface EventSchema<
  K extends InferType<typeof BaseEventKey>,
  V extends InferType<typeof BaseEventValue>
> {
  key: K;
  value: V;
}
