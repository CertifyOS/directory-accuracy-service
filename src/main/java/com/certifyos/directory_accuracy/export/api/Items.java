package com.certifyos.directory_accuracy.export.api;

import java.util.List;

/**
 * A list answer. Every list endpoint wraps its items so paging fields can join later without
 * changing the shape.
 *
 * @param <T> the item type
 */
public record Items<T>(List<T> items) {
    public Items {
        items = items == null ? List.of() : List.copyOf(items);
    }
}
