using System.Text.Json;
using System.Text.Json.Serialization;
using BAMCIS.GeoJSON;

namespace Shared.Services
{
    /// <summary>
    /// System.Text.Json converter for BAMCIS.GeoJSON.Geometry
    /// </summary>
    public class GeometrySystemTextJsonConverter : JsonConverter<Geometry>
    {
        public override Geometry? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            if (reader.TokenType == JsonTokenType.Null)
            {
                return null;
            }

            using var doc = JsonDocument.ParseValue(ref reader);
            var root = doc.RootElement;

            // Use BAMCIS.GeoJSON factory to deserialize
            var json = root.GetRawText();
            return Geometry.FromJson(json);
        }

        public override void Write(Utf8JsonWriter writer, Geometry value, JsonSerializerOptions options)
        {
            if (value == null)
            {
                writer.WriteNullValue();
                return;
            }
            // Use BAMCIS.GeoJSON to serialize
            var json = value.ToJson();
            using var doc = JsonDocument.Parse(json);
            doc.WriteTo(writer);
        }
    }

    public class FeatureIdJsonConverter : JsonConverter<FeatureId>
    {
        public override FeatureId? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            if (reader.TokenType == JsonTokenType.Null)
            {
                return null;
            }

            if (reader.TokenType == JsonTokenType.String)
            {
                var stringValue = reader.GetString()!;
                return new FeatureId(stringValue);
            }
            else if (reader.TokenType == JsonTokenType.Number)
            {
                var intValue = reader.GetInt64();
                return new FeatureId(intValue);
            }
            else
            {
                throw new JsonException("Invalid FeatureId value, must be string or integer.");
            }
        }

        public override void Write(Utf8JsonWriter writer, FeatureId id, JsonSerializerOptions options)
        {
            if (id.Value == null)
            {
                writer.WriteNullValue();
                return;
            }

            writer.WriteStringValue(id.Value);
        }
    }
}
