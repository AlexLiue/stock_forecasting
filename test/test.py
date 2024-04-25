# import the graph_objects function from
# plotly package
import plotly.graph_objects as go

# initialize a Figure object and store it in
# a variable fig
fig = go.Figure()


fig.add_trace(go.Scatter(
    x=[10, 12, 13],
    y=[41, 58, 60],
    name="yaxis1 values"
))

fig.add_trace(go.Scatter(
    x=[12, 13, 14],
    y=[401, 501, 610],
    name="yaxis2 values",
    yaxis="y2"
))

fig.add_trace(go.Scatter(
    x=[14, 15, 16],
    y=[42000, 53000, 65000],
    name="yaxis3 values",
    yaxis="y3"
))

fig.add_trace(go.Scatter(
    x=[15, 16, 17],
    y=[2000, 5000, 7000],
    name="yaxis4 values",
    yaxis="y3"
))

fig.update_layout(
    xaxis=dict(
        domain=[0.08, 0.98]
    ),

    yaxis=dict(
        title="yaxis 1",
        titlefont=dict(
            color="#0000ff"
        ),
        tickfont=dict(
            color="#0000ff"
        )
    ),

    yaxis2=dict(
        title="yaxis 2",
        titlefont=dict(
            color="#FF0000"
        ),
        tickfont=dict(
            color="#FF0000"
        ),
        anchor="free",  # specifying x - axis has to be the fixed
        overlaying="y",  # specifyinfg y - axis has to be separated
        side="left",  # specifying the side the axis should be present
        position=0  # specifying the position of the axis
    ),

    yaxis3=dict(
        title="yaxis 3",
        titlefont=dict(
            color="#006400"
        ),
        tickfont=dict(
            color="#006400"
        ),
        anchor="x",  # specifying x - axis has to be the fixed
        overlaying="y",  # specifyinfg y - axis has to be separated
        side="right",  # specifying the side the axis should be present
        position=1
    )

)

fig.update_layout(
    title_text="4 y-axes scatter plot in plotly",
    width=1000,
    title_x=0.5
)
fig.show()

print('')