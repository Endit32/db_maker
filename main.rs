use rusqlite::{Connection, params, Result};
use std::fs::File;
use std::io::{prelude::*, BufReader};
use std::env;

fn init_db(conn : &Connection) -> Result<()>{
  conn.execute("CREATE TABLE leak_tables (name TEXT, table_name TEXT)", params![])?;
  Ok(())
}

fn create_table(conn : &Connection, table_name : &String) -> Result<()> {
  let mut stmt = conn.prepare("SELECT * FROM leak_tables WHERE table_name = ?")?;
  let result = stmt.exists(params![table_name]);
  if !result.unwrap() {
    conn.execute(format!("CREATE TABLE {} (username, password, other_info)", table_name).as_ref(), params![])?;
    println!("Table created");
    conn.execute("INSERT INTO leak_tables VALUES (?, ?)", params![table_name, table_name])?;
  }
  Ok(())
}

fn populate_table(conn : &Connection, table_name : &String, path : &String, un_i : usize, pw_i : usize, delimiter : &String) -> Result<()> {
  let file = File::open(path).unwrap();
  let reader = BufReader::new(file);
  let mut query = conn.prepare(format!("INSERT INTO {} VALUES (?, ?, ?)", table_name).as_ref())?;
  let mut total = 0;
  for line in reader.lines() {
    let tline = line.unwrap();
    let mut info : Vec<&str> = tline.trim().split(delimiter).collect();
    let un = info.swap_remove(un_i);
    let pw = info.swap_remove(if pw_i > un_i {pw_i-1} else {pw_i});
    let other = if info.len() == 0 {String::from("NULL")} else {info.join(",")};
    let params = params![un, pw, other];
    //if (info.len() == 0) {
    //  params[3] = &Null;
    //} else {
    //  params[3] = &info.join(",");
    //}
    query.execute(params)?;
    total += 1;
    println!("{} queries executed!", total);
  }
  Ok(())
}



fn main() -> Result<()> {
  let args : Vec<String> = env::args().collect();
  if args.len() < 8 {
    println!("Usage: ./{} [database_file] [leaks_file] [table_name] [username_index] [password_index] [delimiter] [should_init]", args[0]);
    return Ok(());
  }
  let conn = Connection::open(&args[1])?;
  if args[7] == "1" {
    init_db(&conn)?;
  }
  create_table(&conn, &args[3])?;
  populate_table(&conn, &args[3], &args[2], args[4].parse::<usize>().unwrap(), args[5].parse::<usize>().unwrap(), &args[6])?;
  Ok(())
}