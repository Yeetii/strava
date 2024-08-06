using System.Collections.Generic;
namespace Shared.Models;

public class PeakInfo{
    public string Id {get; set;}
    public string Name {get; set;}
    public PeakInfo(string id, string name){
        Id = id;
        Name = name;
    }
}
public class Activity
{
    public string Id {get; set;}
    public string Name {get; set;}
    public string Description {get; set;}
    public float? Distance {get; set;}
    public float? MovingTime {get; set;}
    public float? ElapsedTime {get; set;}
    public float? Calories {get; set;}
    public float? TotalElevationGain {get; set;}
    public float? ElevHigh {get; set;}
    public float? ElevLow {get; set;}
    public string SportType {get; set;}
    public System.DateTime? StartDate {get; set;}
    public System.DateTime? StartDateLocal {get; set;}
    public string Timezone {get; set;}
    public IReadOnlyList<float?> StartLatLng {get; set;}
    public IReadOnlyList<float?> EndLatLng {get; set;}
    public int? AthleteCount {get; set;}
    public float? AverageSpeed {get; set;}
    public float? MaxSpeed {get; set;}
    public string Polyline {get; set;}
    public string SummaryPolyline {get; set;}
    public List<PeakInfo> Peaks {get; set;}
}
