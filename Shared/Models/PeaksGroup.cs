namespace Shared.Models
{
    public class PeaksGroup
    {
        public required Guid Id { get; set; }
        public Guid? ParentId { get; set; }
        public required string Name { get; set; }
        public required int AmountOfPeaks { get; set; }
        public required string[] PeakIds { get; set; }
        public Feature? Boundrary { get; set; }
    }
}