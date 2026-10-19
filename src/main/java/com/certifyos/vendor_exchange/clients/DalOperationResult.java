package com.certifyos.vendor_exchange.clients;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * One operation's outcome in a DAL batch answer, as the DAL's {@code OperationResult(alias, result)}
 * sends it. There is no per-operation success flag or error: a failed batch is a 4xx or 5xx status,
 * which {@link DalBatchClient} surfaces as a client exception, and a 200 means every operation ran.
 *
 * @param alias the alias the operation was sent with
 * @param result what the operation returned, filtered by its {@code returning} list: an object for
 *     a write, a list for a find, as the DAL's Jackson reads it
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record DalOperationResult(String alias, Object result) {}
