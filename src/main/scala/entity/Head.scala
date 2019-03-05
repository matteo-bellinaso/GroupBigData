package entity

case class Head(
                 label: String,
                 ref: String,
                 sha: String,
                 user: User,
                 repo: Repo
               )
