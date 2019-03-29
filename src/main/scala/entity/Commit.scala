package entity

case class Commit (
                   sha: String,
                   author: Author,
                   message: String,
                   distinct: Boolean,
                   url: String
                   ) extends Serializable