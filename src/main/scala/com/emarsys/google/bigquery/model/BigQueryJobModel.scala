package com.emarsys.google.bigquery.model

import com.google.api.services.bigquery.model.ErrorProto

object BigQueryJobModel {

  case class BigQueryJobResult(affectedRows: BigInt, jobId: String)

  trait BigQueryErrorReason

  object BigQueryErrorReason {
    def parseFromString(s: String): BigQueryErrorReason = s match {
      case "accessDenied"      => Reasons.AccessDenied
      case "backendError"      => Reasons.BackendError
      case "billingNotEnabled" => Reasons.BillingNotEnabled
      case "blocked"           => Reasons.Blocked
      case "duplicate"         => Reasons.Duplicate
      case "internalError"     => Reasons.InternalError
      case "invalid"           => Reasons.Invalid
      case "invalidQuery"      => Reasons.InvalidQuery
      case "notFound"          => Reasons.NotFound
      case "notImplemented"    => Reasons.NotImplemented
      case "quotaExceeded"     => Reasons.QuotaExceeded
      case "rateLimitExceeded" => Reasons.RateLimitExceeded
      case "resourceInUse"     => Reasons.ResourceInUse
      case "resourcesExceeded" => Reasons.ResourcesExceeded
      case "responseTooLarge"  => Reasons.ResponseTooLarge
      case "stopped"           => Reasons.Stopped
      case "tableUnavailable"  => Reasons.TableUnavailable
      case other               => Reasons.OtherErrorReason(other)
    }
  }

  object Reasons {
    case object AccessDenied                  extends BigQueryErrorReason
    case object BackendError                  extends BigQueryErrorReason
    case object BillingNotEnabled             extends BigQueryErrorReason
    case object Blocked                       extends BigQueryErrorReason
    case object Duplicate                     extends BigQueryErrorReason
    case object InternalError                 extends BigQueryErrorReason
    case object Invalid                       extends BigQueryErrorReason
    case object InvalidQuery                  extends BigQueryErrorReason
    case object NotFound                      extends BigQueryErrorReason
    case object NotImplemented                extends BigQueryErrorReason
    case object QuotaExceeded                 extends BigQueryErrorReason
    case object RateLimitExceeded             extends BigQueryErrorReason
    case object ResourceInUse                 extends BigQueryErrorReason
    case object ResourcesExceeded             extends BigQueryErrorReason
    case object ResponseTooLarge              extends BigQueryErrorReason
    case object Stopped                       extends BigQueryErrorReason
    case object TableUnavailable              extends BigQueryErrorReason
    case class OtherErrorReason(name: String) extends BigQueryErrorReason
  }

  case class BigQueryJobError(
      message: String,
      reason: BigQueryErrorReason,
      location: String,
      table: String,
      jobId: String
  ) extends Throwable(message)

  object BigQueryJobError {
    def apply(
        message: String,
        reason: BigQueryErrorReason,
        location: String,
        table: String,
        jobId: String
    ): BigQueryJobError =
      new BigQueryJobError(message, reason, location, table, jobId)

    def apply(errorResult: ErrorProto, table: String, jobId: String): BigQueryJobError =
      BigQueryJobError(
        errorResult.getMessage,
        BigQueryErrorReason.parseFromString(errorResult.getReason),
        errorResult.getLocation,
        table,
        jobId
      )
  }

  case class UnexpectedBigQueryJobError(cause: Throwable) extends Throwable(cause.getMessage, cause)
}
