package com.certifyos.vendor_exchange.ingestion.api;

import java.util.List;

/**
 * A keyset page: the items and the cursor for the next page. Plain lists use {@code Items} from the
 * export lane instead.
 *
 * @param items the page
 * @param next the cursor to pass back for the next page, or null on the last page
 * @param <T> the item type
 */
public record Page<T>(List<T> items, String next) {
    public Page {
        items = items == null ? List.of() : List.copyOf(items);
    }
}
