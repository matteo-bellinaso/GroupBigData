package entity

case class MainParsed(
                       id: String,
                       `type`: String,
                       actor: Actor,
                       repo: Repo,
                       payload: Payload,
                       publico: Boolean,
                       created_at: String
                     )
