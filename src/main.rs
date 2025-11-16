use std::{
  io::{BufReader, BufRead, Read, Write},
  net::{TcpListener, TcpStream},
  collections::{HashMap},
  thread,
};

fn main() {
  let listener = TcpListener::bind("127.0.0.1:7878").unwrap();

  for stream in listener.incoming() {
    let stream = stream.unwrap();

    thread::spawn(move || {
      handle_connection(stream);
    });
  }
}

#[derive(Debug)]
struct HttpRequest {
  method: String,
  path: String,
  version: String,
  headers: HashMap<String, String>,
  body: String,
}

fn login_user(request: &HttpRequest, stream: &mut TcpStream) -> String {
  println!("Logging in");
  format!("HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\nYES")
}

fn logout_user(request: &HttpRequest, stream: &mut TcpStream) -> String {
  println!("Logging out");
  format!("HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\nYes")
}

fn create_user(request: &HttpRequest, stream: &mut TcpStream) -> String {
  println!("Creating user");
  format!("HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\nYes")
}

fn delete_user(request: &HttpRequest, stream: &mut TcpStream) -> String {
  println!("Deleting user");
  format!("HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\nYes")
}

fn create_room(request: &HttpRequest, stream: &mut TcpStream) -> String {
  println!("Creating room");
  format!("HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\nYes")
}

fn delete_room(request: &HttpRequest, stream: &mut TcpStream) -> String {

  println!("Deleting room");
  format!("HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\nYes")
}

fn join_room(request: &HttpRequest, stream: &mut TcpStream) -> String {
  println!("Joining room");
  format!("HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\nYes")
}

fn leave_room(request: &HttpRequest, stream: &mut TcpStream) -> String {
  println!("Leaving room");
  format!("HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\nYes")
}

fn respond_404() -> String {
  format!("HTTP/1.1 404 Not Found\r\nContent-Length: 9\r\n\r\nNot Found")
}

fn respond_405() -> String {
  format!("HTTP/1.1 405 Method Not Allowed\r\nContent-Length: 0\r\n\r\n")
}

fn handle_connection(mut stream: TcpStream) {
  let peer_addr = stream.peer_addr().unwrap();
  println!("Client connected: {}", peer_addr);

  let mut reader = BufReader::new(stream.try_clone().unwrap());

  loop {
    match parse_http_request(&mut reader) {
      Ok(Some(request)) => {
        println!("--- Parsed Request ---");
        println!("Method: {}", request.method);
        println!("Path: {}", request.path);
        println!("Version: {}", request.version);
        println!("Headers:");
        for (k, v) in &request.headers {
          println!("  {}: {}", k, v);
        }
        println!("Body:\n{}", request.body);
        println!("----------------------");

        let segments: Vec<&str> = request.path.trim_start_matches('/').split('/').collect();

        let response = match (request.method.as_str(), segments.as_slice()) {
          ("POST", ["users"]) => {
            create_user(&request, &mut stream)
          }
          ("DELETE", ["users", user_name]) => {
            delete_user(&request, &mut stream)
          }
          ("POST", ["login"]) => {
            login_user(&request, &mut stream)
          }
          ("POST", ["logout"]) => {
            logout_user(&request, &mut stream)
          }
          ("POST", ["rooms"]) => {
            create_room(&request, &mut stream)
          }
          ("DELETE", ["rooms", room_name]) => {
            delete_room(&request, &mut stream)
          }
          ("POST", ["rooms", room_name, "join"]) => {
            join_room(&request, &mut stream)
          }
          ("POST", ["rooms", room_name, "leave"]) => {
            leave_room(&request, &mut stream)
          }
          _ => respond_404(),
        };

        if let Err(e) = stream.write_all(response.as_bytes()) {
          eprintln!("Write error to {}: {}", peer_addr, e);
          return;
        }
      }
      Ok(None) => {
        // Client disconnected
        println!("Client disconnected: {}", peer_addr);
        return;
      }
      Err(e) => {
        eprintln!("Error parsing request from {}: {}", peer_addr, e);
        return;
      }
    }
  }
}

fn parse_http_request(reader: &mut BufReader<TcpStream>) -> Result<Option<HttpRequest>, std::io::Error> {
  let mut request_line = String::new();
  let bytes_read = reader.read_line(&mut request_line)?;

  if bytes_read == 0 {
    // Connection closed
    return Ok(None);
  }

  let mut parts = request_line.trim().split_whitespace();
  let method = parts.next().unwrap_or("").to_string();
  let path = parts.next().unwrap_or("").to_string();
  let version = parts.next().unwrap_or("").to_string();

  let mut headers = HashMap::new();
  let mut line = String::new();

  // Read headers
  loop {
    line.clear();
    reader.read_line(&mut line)?;
    let line_trimmed = line.trim();
    if line_trimmed.is_empty() {
      break; // End of headers
    }
    if let Some((key, value)) = line_trimmed.split_once(":") {
      headers.insert(
        key.trim().to_string(),
        value.trim().to_string(),
      );
    }
  }

  let mut body = String::new();
  if let Some(content_length) = headers.get("Content-Length") {
    if let Ok(len) = content_length.parse::<usize>() {
      let mut buf = vec![0; len];
      reader.read_exact(&mut buf)?;
      body = String::from_utf8_lossy(&buf).to_string();
    }
  }

  Ok(Some(HttpRequest {
    method,
    path,
    version,
    headers,
    body,
  }))
}
