import { InferType, object, string } from "yup";
import { EventSchema } from "./base-schema";

export const CodeGenerationFailureKey = null;

export const CodeGenerationFailureSchema = object({
  key: object().nullable(),
  value: object({
    buildId: string().required(),
    codeGeneratorVersion: string().required(),
    error: object().optional(),
  }).required(),
});

export type ICodeGenerationFailureSchema = EventSchema<
  InferType<typeof CodeGenerationFailureSchema>
>;
