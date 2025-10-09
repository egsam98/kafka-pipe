defmodule Test.Yaml do
  use ExUnit.Case, async: true

  describe "encode/1" do
    test "normal", do: assert "text" == Yaml.encode!("text")

    test "empty data" do
      error = assert_raise Yaml.ParsingError, fn -> Yaml.encode!(nil) end
      assert %Yaml.ParsingError{message: "nil input"} == error
      assert "" == Yaml.encode!("")
    end
  end

  describe "decode/2" do
    test "normal", do: assert {:ok, "test"} == Yaml.decode("test")

    test "empty data" do
      assert {:error, %Yaml.ParsingError{message: "malformed yaml"}} == Yaml.decode(nil)
      assert {:ok, %{}} == Yaml.decode("")
    end

    test "map option" do
      assert {:ok, %{"field" => 1}} == Yaml.decode("field: 1", map: true)
      assert {:error, %Yaml.ParsingError{message: "Map required"}} == Yaml.decode("field", map: true)
    end
  end
end
