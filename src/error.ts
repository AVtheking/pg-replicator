import { Data } from "effect"

export class PgReplError extends Data.TaggedError("PgReplError")<{
    readonly message: string
    readonly cause?: unknown
}> { }

export class SlotAlreadyExists extends Data.TaggedError("SlotAlreadyExists")<{
    readonly slotName: string
}> { }