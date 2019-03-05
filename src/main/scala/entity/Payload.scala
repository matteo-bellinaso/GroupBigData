package entity

case class Payload (
                   action: String,
                   push_id: BigInt,
                   size: BigInt,
                   distinct_size: BigInt,
                   ref: String,
                   head: String,
                   before: String,
                   commits: Seq[Commit],
                   pull_request: PullRequest
                   )
