export type StationMetadata = {
  name: string;
  location: string;
};

export const STATION_METADATA: Record<string, StationMetadata> = {
  "16018400902": {
    name: "Nith River",
    location: "Grand River St N, Brant Cnty Rd 75, Paris",
  },
  "16018401002": {
    name: "Grand River",
    location: "Glen Morris Rd, Glen Morris",
  },
  "16018401202": {
    name: "Grand River",
    location: "Fountain St S., Blair",
  },
  "16018402702": {
    name: "Grand River",
    location: "Cocksutts Bridge, Erie Ave, Brantford",
  },
  "16018403502": {
    name: "Grand River",
    location: "Dover Rd, Reg Rd 3, Bridge at Dunnville",
  },
  "16018409202": {
    name: "Grand River",
    location: "Haldimand Norfolk Reg Rd 9, York",
  },
  "16018409302": {
    name: "Fairchild Creek",
    location: "Harris Rd, Brantford Twp",
  },
  "16018412802": {
    name: "Big Creek",
    location: "Hwy 54, NW of Caledonia",
  },
};

export function getStationMetadata(stationId: string): StationMetadata {
  return (
    STATION_METADATA[stationId] ?? {
      name: stationId,
      location: "Location details unavailable",
    }
  );
}