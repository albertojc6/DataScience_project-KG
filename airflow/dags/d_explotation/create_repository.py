import requests

with open("moviekg-config.ttl", "rb") as f:
    files = {
        "config": ("moviekg-config.ttl", f, "application/x-turtle"),
    }

    response = requests.post(
        "http://localhost:7200/rest/repositories",
        files=files
    )

    if response.status_code == 201:
        print("Repository created successfully.")
    elif response.status_code == 409:
        print("Repository already exists.")
    else:
        print(f"Error creating repository: {response.status_code}, {response.text}")
