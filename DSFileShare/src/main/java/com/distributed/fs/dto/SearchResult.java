package com.distributed.fs.dto;

import lombok.Getter;
import lombok.Setter;

import java.util.Objects;

@Getter
@Setter
public class SearchResult {
    private String fileName;
    private String location;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SearchResult that = (SearchResult) o;
        return Objects.equals(fileName, that.fileName) && Objects.equals(location, that.location);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fileName, location);
    }
}
