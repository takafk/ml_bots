import torch.nn as nn
import torch.nn.functional as F


CLASS = 10


class ResnetDecoder(nn.Module):
    """
    This class represents the tail of ResNet. It performs a global pooling and maps the output to the
    correct class by using a fully connected layer.
    """

    def __init__(self, in_features, n_classes):
        super().__init__()
        self.avg = nn.AdaptiveAvgPool1d(1)
        self.decoder = nn.Linear(in_features, n_classes)

    def forward(self, x):
        x = self.avg(x)
        x = x.view(x.size(0), -1)
        x = self.decoder(x)
        return x


class BASIC_CNN(nn.Module):
    def __init__(self, total_channel):
        super(BASIC_CNN, self).__init__()

        self.conv1 = nn.Sequential(
            nn.Conv1d(total_channel, 64, kernel_size=2, stride=1),
            nn.ReLU(),
            nn.BatchNorm1d(64),
        )

        self.conv2 = nn.Sequential(
            nn.Conv1d(64, 128, kernel_size=2, stride=1),
            nn.ReLU(),
            nn.BatchNorm1d(128),
        )

        self.decoder = ResnetDecoder(128, CLASS)

    def forward(self, x):

        # First layer
        x = self.conv1(x)

        # Second layer
        x = self.conv2(x)

        # Decoder (connected Layer)
        x = self.decoder(x)
        x = F.softmax(x, dim=1)

        return x
