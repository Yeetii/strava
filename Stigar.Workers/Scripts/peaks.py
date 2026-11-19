import json
import requests

# Script to map peaks by name to an Open Street Map id

def fetch_peak_nodes(peaks):
    base_url = "http://overpass-api.de/api/interpreter"
    query_template = """
    [out:json];
    (
      node["natural"="peak"]["name"="{peak_name}"];
    );
    out body;
    """
    
    for peak in peaks:
        query = query_template.format(peak_name=peak['name'])
        response = requests.post(base_url, data={'data': query})
        
        if response.status_code == 200:
            osm_data = response.json()
            elements = osm_data.get('elements', [])
            for element in elements:
                if 'tags' in element and 'name' in element['tags']:
                    peak['id'] = element['id']
                    peak['lat'] = element['lat']
                    peak['lon'] = element['lon']
        else:
            print(f"Failed to fetch data for peak '{peak}'. Status code: {response.status_code}")
    
    return peaks

with open('jämtlandsfjällen.json', 'r') as f:
    data = json.load(f)

mapped_peaks = fetch_peak_nodes(data)

json_string = json.dumps(mapped_peaks)

with open('output.json', 'w', encoding='utf-8') as file:
    file.write(json_string)