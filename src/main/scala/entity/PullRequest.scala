package entity

case class PullRequest (
                       url: String,
                       id: BigInt,
                       diff_url: String,
                       patch_url: String,
                       issue_url: String,
                       number: BigInt,
                       state: String,
                       locked: String,
                       title: String,
                       user: User,
                       body: String,
                       created_at: String,
                       updated_at: String,
                       closed_at: String,
                       merged_at: String,
                       commits_url: String,
                       review_comment_url: String,
                       comments_url: String,
                       statuses_url: String,
                       head: Head,
                       base: Base
                       )
