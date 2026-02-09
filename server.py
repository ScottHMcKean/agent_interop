from agent_executor import (
    GenieAgentExecutor
)
from a2a.server.apps import A2AStarletteApplication
from starlette.applications import Starlette
from a2a.server.request_handlers import DefaultRequestHandler
from a2a.server.tasks import InMemoryTaskStore
from a2a.types import (
    AgentCapabilities,
    AgentCard,
    AgentSkill,
)
from dotenv import load_dotenv
import os
load_dotenv()

class _MountRootProxy:
    def __init__(self, app: Starlette) -> None:
        self._app = app

    async def __call__(self, scope, receive, send) -> None:
        if scope["type"] != "http":
            await self._app(scope, receive, send)
            return
        new_scope = dict(scope)
        new_scope["root_path"] = f'{scope.get("root_path", "")}{scope["path"]}'
        new_scope["path"] = "/"
        new_scope["raw_path"] = b"/"
        await self._app(new_scope, receive, send)


if __name__ == '__main__':
    skill = AgentSkill(
        id='genie',
        name='Returns genie information',
        description='returns genie information',
        tags=['genie'],
        examples=['List top 3 distribution centers.'],
    )

    # This will be the public-facing agent card
    public_agent_card = AgentCard(
        name='genie-agent',
        description='genie agent',
        url=f'{os.getenv("DATABRICKS_APP_URL")}/a2a',
        version='1.0.0',
        defaultInputModes=['text'],
        defaultOutputModes=['text'],
        capabilities=AgentCapabilities(streaming=True),
        skills=[skill],  
        supportsAuthenticatedExtendedCard=False,
    )

    request_handler = DefaultRequestHandler(
        agent_executor=GenieAgentExecutor(),
        task_store=InMemoryTaskStore(),
    )

    server = A2AStarletteApplication(agent_card=public_agent_card,
                                     http_handler=request_handler,
                                     )
    app = server.build(rpc_url = "/")
    main_app = Starlette()
    main_app.add_route("/a2a", _MountRootProxy(app))
    main_app.mount("/a2a", app)
    import uvicorn

    uvicorn.run(main_app, host='0.0.0.0', port=8000)
