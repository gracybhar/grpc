/*
 *
 * Copyright 2015 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

#include <grpc/grpc.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <iostream>
#include <memory>
#include <string>

#include "helper.h"
#include "absl/flags/parse.h"
#include "absl/log/initialize.h"
#ifdef BAZEL_BUILD
#include "examples/protos/route_guide.grpc.pb.h"
#else
#include "route_guide.grpc.pb.h"
#endif

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerReaderWriter;
using grpc::ServerWriter;
using grpc::Status;
using routeguide::Feature;
using routeguide::Point;
using routeguide::Rectangle;
using routeguide::RouteGuide;
using routeguide::RouteNote;
using routeguide::RouteSummary;
using std::chrono::system_clock;

float ConvertToRadians(float num) { return num * 3.1415926 / 180; }

// The formula is based on http://mathforum.org/library/drmath/view/51879.html
float GetDistance(const Point& start, const Point& end) {
  const float kCoordFactor = 10000000.0;
  float lat_1 = start.latitude() / kCoordFactor;
  float lat_2 = end.latitude() / kCoordFactor;
  float lon_1 = start.longitude() / kCoordFactor;
  float lon_2 = end.longitude() / kCoordFactor;
  float lat_rad_1 = ConvertToRadians(lat_1);
  float lat_rad_2 = ConvertToRadians(lat_2);
  float delta_lat_rad = ConvertToRadians(lat_2 - lat_1);
  float delta_lon_rad = ConvertToRadians(lon_2 - lon_1);

  float a = pow(sin(delta_lat_rad / 2), 2) +
            cos(lat_rad_1) * cos(lat_rad_2) * pow(sin(delta_lon_rad / 2), 2);
  float c = 2 * atan2(sqrt(a), sqrt(1 - a));
  int R = 6371000;  // metres

  return R * c;
}

std::string GetFeatureName(const Point& point,
                           const std::vector<Feature>& feature_list) {
  for (const Feature& f : feature_list) {
    if (f.location().latitude() == point.latitude() &&
        f.location().longitude() == point.longitude()) {
      return f.name();
    }
  }
  return "";
}

// this class implements the generated RouteGuide::Service interface
// this provides the default gRPC server behavior
// THIS IMPLEMENTS ALL OF OUR SERVICE METHODS
class RouteGuideImpl final : public RouteGuide::Service {
 public:
  explicit RouteGuideImpl(const std::string& db) {
    routeguide::ParseDb(db, &feature_list_);
  }

  // This gets a Point from the client and returns the corresponding feature
  // information from its database in a Feature
  //    we pass a context object for the RPS (Point protocol buffer request)
  //    and a Feature protocol buffer to fill in with the response information
  //    we populate Feature with the appropriate information and then return the
  //    Feature to client

  Status GetFeature(ServerContext* context, const Point* point,
                    Feature* feature) override {
    feature->set_name(GetFeatureName(*point, feature_list_));
    feature->mutable_location()->CopyFrom(*point);
    return Status::OK;
  }

  // this is a server-side streaming RPC so we need to send back multiple
  // Features to our client
  // this time we get a request object (Rectangle) and a special ServerWriter
  // object
  Status ListFeatures(ServerContext* context,
                      const routeguide::Rectangle* rectangle,
                      ServerWriter<Feature>* writer) override {
    auto lo = rectangle->lo();
    auto hi = rectangle->hi();
    long left = (std::min)(lo.longitude(), hi.longitude());
    long right = (std::max)(lo.longitude(), hi.longitude());
    long top = (std::max)(lo.latitude(), hi.latitude());
    long bottom = (std::min)(lo.latitude(), hi.latitude());
    // we populate as many Feature objects as we need to return, writing them to
    // the ServerWriter and then return to tell gRPC that we have finished
    // writing responses
    for (const Feature& f : feature_list_) {
      if (f.location().longitude() >= left &&
          f.location().longitude() <= right &&
          f.location().latitude() >= bottom && f.location().latitude() <= top) {
        writer->Write(f);
      }
    }
    return Status::OK;
  }

  // same as above but client side stream but ServerReader instead and single
  // response
  Status RecordRoute(ServerContext* context, ServerReader<Point>* reader,
                     RouteSummary* summary) override {
    Point point;
    int point_count = 0;
    int feature_count = 0;
    float distance = 0.0;
    Point previous;

    system_clock::time_point start_time = system_clock::now();
    // we use the ServerReader's Read() method to read in our client's requests
    // to a request object (Point) until no more messages and server continues
    // reading until message stream returns false
    while (reader->Read(&point)) {
      point_count++;
      if (!GetFeatureName(point, feature_list_).empty()) {
        feature_count++;
      }
      if (point_count != 1) {
        distance += GetDistance(previous, point);
      }
      previous = point;
    }
    system_clock::time_point end_time = system_clock::now();
    summary->set_point_count(point_count);
    summary->set_feature_count(feature_count);
    summary->set_distance(static_cast<long>(distance));
    auto secs =
        std::chrono::duration_cast<std::chrono::seconds>(end_time - start_time);
    summary->set_elapsed_time(secs.count());

    return Status::OK;
  }

  // bidirectional streaming RPC
  // now we have a ServerReaderWriter that can be used to read and write
  // messages each side will always get messages in the order they were written
  // and both client and server can read and write in any order the streams
  // operate independently
  Status RouteChat(ServerContext* context,
                   ServerReaderWriter<RouteNote, RouteNote>* stream) override {
    RouteNote note;
    while (stream->Read(&note)) {
      std::unique_lock<std::mutex> lock(mu_);
      for (const RouteNote& n : received_notes_) {
        if (n.location().latitude() == note.location().latitude() &&
            n.location().longitude() == note.location().longitude()) {
          stream->Write(n);
        }
      }
      // recievited_notes_ uys is an instance variable declared above and can be
      // accessed by multiple threads
      received_notes_.push_back(note);
    }

    return Status::OK;
  }

 private:
  std::vector<Feature> feature_list_;
  std::mutex mu_;
  std::vector<RouteNote> received_notes_;
};

// now that we have implemented all of our methods (that were delared in .proto)
// we need to start up the gRPC server so clients can actually use the service
void RunServer(const std::string& db_path) {
  std::string server_address("0.0.0.0:50051");
  // create an instance of our service implementation class
  RouteGuideImpl service(db_path);

  // we build and start our server using a ServerBuilder
  ServerBuilder builder;
  // specify the address and port we want to use to listen for client requests
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  // register our service implementation with the builder
  builder.RegisterService(&service);
  // call BuildAndStart() on the builder to create and start an RPC server for
  // our service
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;
  // call Wait() on the server to do a blocking wait until process is killed
  server->Wait();
}

int main(int argc, char** argv) {
  absl::ParseCommandLine(argc, argv);
  absl::InitializeLog();
  // Expect only arg: --db_path=path/to/route_guide_db.json.
  std::string db = routeguide::GetDbFileContent(argc, argv);
  RunServer(db);

  return 0;
}
