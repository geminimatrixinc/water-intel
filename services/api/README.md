# API service

Canonical FastAPI files live here.

- App entrypoint: `services/api/main.py`
- Dependencies: `services/api/requirements.txt`
- Compatibility shim: `api/main.py`

Legacy local startup still works:

```powershell
uvicorn api.main:app --reload
```
