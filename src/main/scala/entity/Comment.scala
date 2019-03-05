package entity

case class Comment (
                   url: String,
                   pull_request_review_id: BigInt,
                   id: BigInt,
                   diff_hunk: String,
                   path: String,
                   position: BigInt,
                   original_position: BigInt,
                   commit_id: String,
                   original_commit_id: String,
                   user: User,
                   body: String,
                   created_at: String,
                   updated_at: String,
                   pull_request_url: String,
                   author_association: String,
                   _links: Links
                   )
