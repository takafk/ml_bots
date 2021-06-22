import torch.nn as nn


CLASS = 10


class SimpleCNNDecoder(nn.Module):
    def __init__(self, in_features, n_classes):
        super().__init__()
        self.avg = nn.AdaptiveAvgPool1d(1)
        self.decoder = nn.Linear(in_features, n_classes)

    def forward(self, x):
        x = self.avg(x)
        x = x.view(x.size(0), -1)
        x = self.decoder(x)
        return x


class ResBlock(nn.Module):
    def __init__(self, in_channel, out_channel):
        super().__init__()
        channel = out_channel // 4

        self.conv = nn.Sequential(
            # Convolution of 1
            nn.Conv1d(in_channel, channel, kernel_size=1, stride=1),
            nn.ReLU(),
            # Convolution of 3
            nn.Conv1d(channel, channel, kernel_size=3, stride=1, padding=1),
            nn.ReLU(),
            # Convolution of 1
            nn.Conv1d(channel, out_channel, kernel_size=1, stride=1),
        )

        self.relu = nn.ReLU()

        # skip connection
        self.shortcut = self._shortcut(in_channel, out_channel)

    def forward(self, x):

        h = self.conv(x)
        shortcut = self.shortcut(x)
        y = self.relu(h + shortcut)

        return y

    def _shortcut(self, in_channel, out_channel):
        if in_channel != out_channel:
            return nn.Conv1d(in_channel, out_channel, kernel_size=1, padding=0)
        else:
            return lambda x: x


class ResNet1D(nn.Module):
    """Residual Net without Batch Normalization and Pooling.

    Notes:
    - We remove batch normalization to avoid leakage.
    - We remove pooling to keep information.
    """

    def __init__(self, total_channel, n_classes):
        super().__init__()

        self.conv1 = nn.Conv1d(total_channel, 64, kernel_size=7, stride=2, padding=3)
        self.relu1 = nn.ReLU()

        # Block 1
        self.block0 = self._building_block(256, channel_in=64)
        self.block1 = nn.ModuleList([self._building_block(256) for _ in range(2)])
        self.conv2 = nn.Conv1d(256, 512, kernel_size=1, stride=2)
        # Block 2
        self.block2 = nn.ModuleList([self._building_block(512) for _ in range(4)])
        self.conv3 = nn.Conv1d(512, 1024, kernel_size=1, stride=2)
        # Block 3
        self.block3 = nn.ModuleList([self._building_block(1024) for _ in range(6)])
        self.conv4 = nn.Conv1d(1024, 2048, kernel_size=1, stride=2)
        # Block 4
        self.block4 = nn.ModuleList([self._building_block(2048) for _ in range(3)])
        self.decoder = self._decoder(2048, n_classes)

    def forward(self, x):
        h = self.conv1(x)
        h = self.relu1(h)
        h = self.block0(h)
        for block in self.block1:
            h = block(h)
        h = self.conv2(h)
        for block in self.block2:
            h = block(h)
        h = self.conv3(h)
        for block in self.block3:
            h = block(h)
        h = self.conv4(h)
        for block in self.block4:
            h = block(h)
        y = self.decoder(h)
        return y

    def _building_block(self, channel_out, channel_in=None):
        if channel_in is None:
            channel_in = channel_out
        return ResBlock(channel_in, channel_out)

    def _decoder(self, in_channel, n_classes):
        return SimpleCNNDecoder(in_channel, n_classes)
