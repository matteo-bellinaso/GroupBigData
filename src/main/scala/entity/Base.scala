package entity

case class Base(
               label: String,
               ref: String,
               sha: String,
               user: User,
               repo: Repo
               )