package ly.bit.classifier.domain;

public record CacheTuple<T>(String key, T value, boolean error) {
    public CacheTuple(String key) {
        this(key, null, false);
    }

    public CacheTuple(String key, T value) {
        this(key, value, false);
    }

    public CacheTuple(String key, boolean error) {
        this(key, null, error);
    }
}
