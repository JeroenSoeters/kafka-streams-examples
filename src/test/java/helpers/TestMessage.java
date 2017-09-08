package helpers;

public class TestMessage {
    private String data;

    public TestMessage(String data) {
        this.data = data;
    }

    public String getData() { return data; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TestMessage that = (TestMessage) o;

        return data != null ? data.equals(that.data) : that.data == null;

    }

    @Override
    public int hashCode() {
        return data != null ? data.hashCode() : 0;
    }
}
